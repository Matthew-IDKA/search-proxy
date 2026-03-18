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
    yield
    search_proxy._client = None
    search_proxy._active_requests = 0
    search_proxy._waiting_requests = 0


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
