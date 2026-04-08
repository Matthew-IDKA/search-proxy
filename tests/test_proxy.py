"""Tests for search-proxy rate-limiting service."""

import asyncio

import httpx
import pytest
import search_proxy
from httpx import ASGITransport


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def mock_upstream():
    """Provide a mock that tracks concurrency."""
    state = {"active": 0, "peak": 0, "total": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        state["active"] += 1
        state["total"] += 1
        if state["active"] > state["peak"]:
            state["peak"] = state["active"]
        await asyncio.sleep(0.1)  # Simulate upstream latency
        state["active"] -= 1
        return httpx.Response(
            200,
            json={"query": str(request.url.params.get("q", "")), "results": []},
            headers={"content-type": "application/json"},
        )

    transport = httpx.MockTransport(handler)
    state["transport"] = transport
    return state


@pytest.fixture
def timeout_client():
    """Client whose get() raises TimeoutException."""
    client = httpx.AsyncClient()

    async def timeout_get(*args, **kwargs):
        raise httpx.ReadTimeout("Upstream timed out")

    client.get = timeout_get
    return client


@pytest.fixture
def param_tracking_upstream():
    """Upstream that echoes back all query params."""
    captured = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.update(dict(request.url.params))
        return httpx.Response(
            200,
            json={"params": dict(request.url.params)},
            headers={"content-type": "application/json"},
        )

    return captured, httpx.MockTransport(handler)


def _inject_client(mock_transport, timeout=30):
    """Set a mock client into the search_proxy module."""
    client = httpx.AsyncClient(transport=mock_transport, timeout=timeout)
    search_proxy._client = client
    return client


@pytest.fixture(autouse=True)
def _cleanup():
    """Reset module state after each test."""
    # Disable jitter during tests to keep timing assertions stable
    original_jitter_min = search_proxy.JITTER_MIN
    original_jitter_max = search_proxy.JITTER_MAX
    search_proxy.JITTER_MIN = 0.0
    search_proxy.JITTER_MAX = 0.0
    yield
    search_proxy._client = None
    search_proxy._active_requests = 0
    search_proxy._waiting_requests = 0
    search_proxy._engine_status = {}
    search_proxy._total_queries = 0
    search_proxy._failed_queries = 0
    search_proxy._last_query_time = 0.0
    # Reset rate limiter to default for each test
    search_proxy._rate_limiter = search_proxy.TokenBucket(
        search_proxy.RATE_LIMIT, search_proxy.RATE_BURST
    )
    search_proxy.JITTER_MIN = original_jitter_min
    search_proxy.JITTER_MAX = original_jitter_max


@pytest.mark.anyio
async def test_search_passthrough(mock_upstream):
    """Single request returns SearXNG response unchanged."""
    mock_client = _inject_client(mock_upstream["transport"])

    transport = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/search", params={"q": "hello", "format": "json"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["query"] == "hello"
    assert "results" in data
    assert mock_upstream["total"] == 1

    await mock_client.aclose()


@pytest.mark.anyio
async def test_concurrent_limiting(mock_upstream):
    """Fire 8 requests simultaneously, verify max 4 are in-flight at once."""
    mock_client = _inject_client(mock_upstream["transport"])

    transport = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        tasks = [
            client.get("/search", params={"q": f"query-{i}", "format": "json"})
            for i in range(8)
        ]
        results = await asyncio.gather(*tasks)

    assert all(r.status_code == 200 for r in results)
    assert mock_upstream["total"] == 8
    assert mock_upstream["peak"] <= search_proxy.MAX_CONCURRENT

    await mock_client.aclose()


@pytest.mark.anyio
async def test_health_endpoint():
    """Health endpoint returns status, queue_depth, active_requests."""
    transport = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")

    assert resp.status_code == 200
    data = resp.json()
    assert "status" in data
    assert "queue_depth" in data
    assert "active_requests" in data
    assert data["status"] == "ok"
    assert isinstance(data["queue_depth"], int)
    assert isinstance(data["active_requests"], int)


@pytest.mark.anyio
async def test_timeout_handling(timeout_client):
    """Upstream timeout returns 504."""
    search_proxy._client = timeout_client

    transport = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/search", params={"q": "slow", "format": "json"}, timeout=10
        )

    assert resp.status_code == 504
    data = resp.json()
    assert "timeout" in data["detail"].lower() or "timed out" in data["detail"].lower()

    await timeout_client.aclose()


@pytest.mark.anyio
async def test_query_params_forwarded(param_tracking_upstream):
    """All query params are passed through to upstream."""
    captured, transport_mock = param_tracking_upstream
    mock_client = _inject_client(transport_mock)

    transport = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/search",
            params={
                "q": "test query",
                "format": "json",
                "language": "en",
                "categories": "general",
            },
        )

    assert resp.status_code == 200
    assert captured["q"] == "test query"
    assert captured["format"] == "json"
    assert captured["language"] == "en"
    assert captured["categories"] == "general"

    await mock_client.aclose()


# === Engine status tracking tests ===


def _make_searxng_transport(results_count=5, unresponsive=None):
    """Create a mock SearXNG that returns configurable engine status."""
    if unresponsive is None:
        unresponsive = []

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "query": str(request.url.params.get("q", "")),
                "number_of_results": results_count,
                "results": [{"title": f"Result {i}"} for i in range(results_count)],
                "unresponsive_engines": unresponsive,
            },
            headers={"content-type": "application/json"},
        )

    return httpx.MockTransport(handler)


@pytest.mark.anyio
async def test_engine_status_tracks_suspensions():
    """Unresponsive engines from SearXNG are tracked in _engine_status."""
    transport = _make_searxng_transport(
        results_count=1,
        unresponsive=[
            ["google", "HTTP error 403"],
            ["duckduckgo", "CAPTCHA"],
        ],
    )
    mock_client = _inject_client(transport)

    asgi = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=asgi, base_url="http://test") as client:
        resp = await client.get("/search", params={"q": "test", "format": "json"})

    assert resp.status_code == 200
    assert search_proxy._engine_status["google"]["status"] == "suspended"
    assert search_proxy._engine_status["google"]["reason"] == "HTTP error 403"
    assert search_proxy._engine_status["duckduckgo"]["status"] == "suspended"
    assert search_proxy._engine_status["duckduckgo"]["reason"] == "CAPTCHA"

    await mock_client.aclose()


@pytest.mark.anyio
async def test_engine_status_tracks_recovery():
    """Engine marked as recovered when it disappears from unresponsive list."""
    # First query: google is suspended
    transport1 = _make_searxng_transport(
        results_count=1,
        unresponsive=[["google", "HTTP error 403"]],
    )
    mock_client = _inject_client(transport1)

    asgi = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=asgi, base_url="http://test") as client:
        await client.get("/search", params={"q": "test1", "format": "json"})

    assert search_proxy._engine_status["google"]["status"] == "suspended"
    await mock_client.aclose()

    # Second query: google is no longer in unresponsive
    transport2 = _make_searxng_transport(results_count=5, unresponsive=[])
    mock_client2 = _inject_client(transport2)

    async with httpx.AsyncClient(transport=asgi, base_url="http://test") as client:
        await client.get("/search", params={"q": "test2", "format": "json"})

    assert search_proxy._engine_status["google"]["status"] == "ok"
    await mock_client2.aclose()


@pytest.mark.anyio
async def test_empty_results_counted():
    """Queries returning zero results increment _failed_queries."""
    transport = _make_searxng_transport(results_count=0, unresponsive=[])
    mock_client = _inject_client(transport)

    asgi = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=asgi, base_url="http://test") as client:
        await client.get("/search", params={"q": "test", "format": "json"})

    assert search_proxy._total_queries == 1
    assert search_proxy._failed_queries == 1
    await mock_client.aclose()


@pytest.mark.anyio
async def test_health_reports_engine_status():
    """Health endpoint includes engine status and suspended details."""
    # Inject a suspension
    search_proxy._engine_status = {
        "google": {"status": "suspended", "reason": "HTTP error 403", "since": 0.0},
        "bing": {"status": "ok", "reason": "", "since": 0.0},
    }
    search_proxy._total_queries = 10
    search_proxy._failed_queries = 3

    asgi = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=asgi, base_url="http://test") as client:
        resp = await client.get("/health")

    data = resp.json()
    assert data["status"] == "degraded"
    assert data["engines"]["google"] == "suspended"
    assert data["engines"]["bing"] == "ok"
    assert data["suspended_engines"]["google"] == "HTTP error 403"
    assert "bing" not in data["suspended_engines"]
    assert data["total_queries"] == 10
    assert data["empty_queries"] == 3


@pytest.mark.anyio
async def test_health_ok_when_no_suspensions():
    """Health reports 'ok' when no engines are suspended."""
    search_proxy._engine_status = {
        "bing": {"status": "ok", "reason": "", "since": 0.0},
    }

    asgi = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=asgi, base_url="http://test") as client:
        resp = await client.get("/health")

    data = resp.json()
    assert data["status"] == "ok"
    assert data["suspended_engines"] == {}


# === Rate limiter tests ===


@pytest.mark.anyio
async def test_token_bucket_single_request_immediate():
    """Single request after idle completes without delay."""
    bucket = search_proxy.TokenBucket(rate=1.0, burst=2)
    start = asyncio.get_event_loop().time()
    await bucket.acquire()
    elapsed = asyncio.get_event_loop().time() - start
    assert elapsed < 0.1  # should be near-instant


@pytest.mark.anyio
async def test_token_bucket_burst_then_throttle():
    """Burst of requests takes longer than burst count * 0, proving throttling."""
    bucket = search_proxy.TokenBucket(rate=10.0, burst=2)  # 10/s for fast test
    start = asyncio.get_event_loop().time()
    for _ in range(6):
        await bucket.acquire()
    total = asyncio.get_event_loop().time() - start

    # 2 burst + 4 throttled at 10/s = ~0.4s minimum
    # Allow some slack but it must be meaningfully throttled
    assert total > 0.2


@pytest.mark.anyio
async def test_token_bucket_disabled_when_rate_zero():
    """Rate=0 disables the limiter — all requests immediate."""
    bucket = search_proxy.TokenBucket(rate=0, burst=2)
    times = []
    for _ in range(10):
        start = asyncio.get_event_loop().time()
        await bucket.acquire()
        times.append(asyncio.get_event_loop().time() - start)
    assert all(t < 0.05 for t in times)


@pytest.mark.anyio
async def test_rate_limiter_in_search_flow():
    """Rate limiter is active in the search request path."""
    # Use a fast rate limiter for testing
    search_proxy._rate_limiter = search_proxy.TokenBucket(rate=20.0, burst=1)
    transport = _make_searxng_transport(results_count=3)
    mock_client = _inject_client(transport)

    asgi = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=asgi, base_url="http://test") as client:
        start = asyncio.get_event_loop().time()
        tasks = [
            client.get("/search", params={"q": f"q{i}", "format": "json"})
            for i in range(4)
        ]
        results = await asyncio.gather(*tasks)
        elapsed = asyncio.get_event_loop().time() - start

    assert all(r.status_code == 200 for r in results)
    # With burst=1 and rate=20/s, 4 requests should take >0.1s (3 throttled at 0.05s each)
    assert elapsed > 0.05
    await mock_client.aclose()


@pytest.mark.anyio
async def test_health_reports_rate_config():
    """Health endpoint includes rate limiter configuration."""
    asgi = ASGITransport(app=search_proxy.app)
    async with httpx.AsyncClient(transport=asgi, base_url="http://test") as client:
        resp = await client.get("/health")

    data = resp.json()
    assert "rate_limit_rps" in data
    assert "rate_burst" in data
    assert isinstance(data["rate_limit_rps"], (int, float))
    assert isinstance(data["rate_burst"], int)
