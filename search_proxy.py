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

# === State ===
_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
_active_requests = 0
_waiting_requests = 0
_client: httpx.AsyncClient | None = None


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
        "search-proxy started: upstream=%s, max_concurrent=%d, port=%d",
        UPSTREAM_URL,
        MAX_CONCURRENT,
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
            _waiting_requests -= 1
            _active_requests += 1
            logger.info(
                "Request started: q=%s (active=%d)", params.get("q", ""), _active_requests
            )
            try:
                upstream_resp = await client.get(
                    f"{UPSTREAM_URL}/search", params=params
                )
                return JSONResponse(
                    content=upstream_resp.json(),
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
    """Return service health with queue metrics."""
    return {
        "status": "ok",
        "active_requests": _active_requests,
        "queue_depth": _waiting_requests,
        "max_concurrent": MAX_CONCURRENT,
        "upstream": UPSTREAM_URL,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("search_proxy:app", host="0.0.0.0", port=PORT, log_level="info")
