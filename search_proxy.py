"""Search query rate-limiting proxy for SearXNG."""

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# === Configuration ===
UPSTREAM_URL = os.getenv("UPSTREAM_URL", "http://localhost:8086")
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "4"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
LOG_FILE = os.getenv("LOG_FILE", "/var/log/search-proxy/proxy.log")
PORT = int(os.getenv("PORT", "8088"))
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")
RATE_LIMIT = float(os.getenv("RATE_LIMIT", "0.5"))  # requests per second (0 = disabled)
RATE_BURST = int(os.getenv("RATE_BURST", "2"))  # burst allowance
JITTER_MIN = float(os.getenv("JITTER_MIN", "0.5"))  # min random delay (seconds)
JITTER_MAX = float(os.getenv("JITTER_MAX", "2.5"))  # max random delay (seconds)

# === Logging (dual: file + stdout) ===
logger = logging.getLogger("search-proxy")
logger.setLevel(logging.INFO)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
)
logger.addHandler(stdout_handler)

try:
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logger.addHandler(file_handler)
except OSError:
    logger.warning("Cannot write to %s, file logging disabled", LOG_FILE)

# === Token Bucket Rate Limiter ===
class TokenBucket:
    """Limits throughput to `rate` requests/second with `burst` burst allowance."""

    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.burst = burst
        self._tokens = float(burst)
        self._last_refill: float | None = None
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        import random

        if self.rate <= 0:
            return  # disabled
        async with self._lock:
            loop = asyncio.get_event_loop()
            now = loop.time()
            if self._last_refill is not None:
                elapsed = now - self._last_refill
                self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._last_refill = now
            if self._tokens < 1:
                wait = (1 - self._tokens) / self.rate
                await asyncio.sleep(wait)
                self._tokens = 0
            else:
                self._tokens -= 1
            # Jitter: randomized delay to break uniform timing patterns
            jitter = random.uniform(JITTER_MIN, JITTER_MAX)
            await asyncio.sleep(jitter)


# === State ===
_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
_rate_limiter = TokenBucket(RATE_LIMIT, RATE_BURST)
_active_requests = 0
_waiting_requests = 0
_client: httpx.AsyncClient | None = None

# Engine health: {engine_name: {"status": "ok"|"suspended", "reason": str, "since": float}}
_engine_status: dict[str, dict] = {}
_last_query_time: float = 0.0
_total_queries = 0
_failed_queries = 0  # queries returning zero results


def _update_engine_status(response_json: dict) -> None:
    """Extract engine health from SearXNG response. Called on every proxied query."""
    global _last_query_time, _total_queries, _failed_queries

    _last_query_time = asyncio.get_event_loop().time()
    _total_queries += 1

    if response_json.get("number_of_results", -1) == 0:
        _failed_queries += 1

    unresponsive = response_json.get("unresponsive_engines", [])
    suspended_names = set()

    for entry in unresponsive:
        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
            name, reason = entry[0], entry[1]
            suspended_names.add(name)
            was_ok = _engine_status.get(name, {}).get("status") == "ok"
            _engine_status[name] = {
                "status": "suspended",
                "reason": str(reason),
                "since": _last_query_time,
            }
            if was_ok or name not in _engine_status:
                logger.warning(
                    "Engine suspended: %s (%s)", name, reason
                )

    for name in list(_engine_status):
        if name not in suspended_names and _engine_status[name]["status"] == "suspended":
            logger.info("Engine recovered: %s", name)
            _engine_status[name] = {"status": "ok", "reason": "", "since": _last_query_time}


async def _push_metrics() -> None:
    """Push engine status metrics to Prometheus Pushgateway. Best-effort, non-blocking."""
    if not _client or PUSHGATEWAY_URL == "http://localhost:9091":
        return

    lines = []
    for name, info in _engine_status.items():
        value = 0 if info["status"] == "suspended" else 1
        lines.append(f'inhale_engine_up{{engine="{name}"}} {value}')
    lines.append(f"inhale_search_queries_total {_total_queries}")
    lines.append(f"inhale_search_queries_empty_total {_failed_queries}")

    body = "\n".join(lines) + "\n"
    try:
        await _client.put(
            f"{PUSHGATEWAY_URL}/metrics/job/inhale/instance/search-proxy",
            content=body,
            headers={"content-type": "text/plain"},
            timeout=5,
        )
    except Exception as exc:
        logger.debug("Pushgateway push failed (non-fatal): %s", exc)


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Startup/shutdown lifecycle."""
    global _client

    # Fail-fast: check upstream is reachable
    logger.info("Checking upstream connectivity: %s", UPSTREAM_URL)
    try:
        async with httpx.AsyncClient() as probe:
            resp = await probe.get(f"{UPSTREAM_URL}/healthz", timeout=5)
            logger.info("Upstream responded: %d", resp.status_code)
    except Exception:
        logger.warning(
            "Upstream %s not reachable at startup (may come up later)",
            UPSTREAM_URL,
        )

    _client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)
    logger.info(
        "search-proxy started: upstream=%s, max_concurrent=%d, rate_limit=%.1f/s, burst=%d, port=%d",
        UPSTREAM_URL,
        MAX_CONCURRENT,
        RATE_LIMIT,
        RATE_BURST,
        PORT,
    )

    yield

    await _client.aclose()
    _client = None
    logger.info("search-proxy shut down")


app = FastAPI(lifespan=lifespan)


@app.get("/search")
async def search(request: Request):
    """Proxy search request to SearXNG with concurrency limiting."""
    global _active_requests, _waiting_requests

    params = dict(request.query_params)
    assert _client is not None, "Client not initialized"
    client = _client

    _waiting_requests += 1
    logger.info("Request queued: q=%s (waiting=%d)", params.get("q", ""), _waiting_requests)

    try:
        async with _semaphore:
            await _rate_limiter.acquire()
            _waiting_requests -= 1
            _active_requests += 1
            logger.info(
                "Request started: q=%s (active=%d)", params.get("q", ""), _active_requests
            )
            try:
                upstream_resp = await client.get(
                    f"{UPSTREAM_URL}/search", params=params
                )
                resp_json = upstream_resp.json()
                _update_engine_status(resp_json)
                asyncio.create_task(_push_metrics())
                return JSONResponse(
                    content=resp_json,
                    status_code=upstream_resp.status_code,
                    headers={
                        k: v
                        for k, v in upstream_resp.headers.items()
                        if k.lower()
                        not in ("content-encoding", "transfer-encoding", "content-length")
                    },
                )
            except httpx.TimeoutException:
                logger.warning("Upstream timeout: q=%s", params.get("q", ""))
                return JSONResponse(
                    status_code=504,
                    content={"detail": "Upstream request timed out"},
                )
            except Exception as exc:
                logger.error("Upstream error: %s", exc)
                return JSONResponse(
                    status_code=502,
                    content={"detail": f"Upstream error: {exc}"},
                )
            finally:
                _active_requests -= 1
    except Exception:
        _waiting_requests -= 1
        raise


@app.get("/")
async def root():
    """Root endpoint for health probes."""
    return await health()


@app.get("/health")
async def health():
    """Return service health with queue metrics and engine status."""
    suspended = {
        name: info["reason"]
        for name, info in _engine_status.items()
        if info["status"] == "suspended"
    }
    return {
        "status": "degraded" if suspended else "ok",
        "active_requests": _active_requests,
        "queue_depth": _waiting_requests,
        "max_concurrent": MAX_CONCURRENT,
        "upstream": UPSTREAM_URL,
        "engines": {
            name: info["status"] for name, info in _engine_status.items()
        },
        "suspended_engines": suspended,
        "total_queries": _total_queries,
        "empty_queries": _failed_queries,
        "rate_limit_rps": RATE_LIMIT,
        "rate_burst": RATE_BURST,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("search_proxy:app", host="0.0.0.0", port=PORT, log_level="info")
