# search-proxy

A rate-limiting proxy for [SearXNG](https://github.com/searxng/searxng) that prevents upstream search engine throttling when AI agents fire many concurrent queries.

## Problem

AI coding agents (Claude Code, Open WebUI, etc.) send parallel search queries through SearXNG. Each SearXNG query fans out to multiple upstream engines (Google, Bing, DuckDuckGo). Burst requests overwhelm upstream rate limits, triggering CAPTCHAs and empty results.

## Solution

search-proxy sits between your AI tools and SearXNG, limiting concurrent upstream queries with an async semaphore. Excess requests queue in memory and drain as slots free. Callers see slower responses under load instead of failures.

```
AI Agent --> search-proxy:8086 --> SearXNG:8089 --> Google/Bing/DDG
                (max 4 concurrent)
```

## Quick Start

```bash
docker compose up -d
```

By default, the proxy listens on port 8088 and forwards to `http://searxng:8080`. Configure via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `UPSTREAM_URL` | `http://searxng:8080` | SearXNG base URL |
| `MAX_CONCURRENT` | `4` | Max simultaneous upstream queries |
| `REQUEST_TIMEOUT` | `30` | Upstream timeout in seconds |
| `PORT` | `8088` | Proxy listen port |
| `LOG_FILE` | `/var/log/search-proxy/proxy.log` | Log file path |

## Endpoints

- `GET /search?q=QUERY&format=json` -- proxied to SearXNG with rate limiting. All query parameters are forwarded as-is.
- `GET /health` -- returns `{"status": "ok", "active_requests": N, "queue_depth": N, "max_concurrent": N}`

## Deployment

### Same Docker network as SearXNG

```yaml
services:
  search-proxy:
    build: .
    ports:
      - "8086:8088"
    environment:
      - UPSTREAM_URL=http://searxng:8080
      - MAX_CONCURRENT=4
```

### SearXNG on host network

```yaml
services:
  search-proxy:
    build: .
    ports:
      - "8086:8088"
    environment:
      - UPSTREAM_URL=http://host.docker.internal:8089
      - MAX_CONCURRENT=4
```

Point your DNS or client config to the proxy port instead of SearXNG directly.

## How it works

- FastAPI with `asyncio.Semaphore(MAX_CONCURRENT)` gates upstream requests
- Excess requests queue in memory (no Redis/database needed)
- Callers block until their request completes (no polling or job IDs)
- Health endpoint exposes real-time queue depth and active request count
- Dual logging to stdout and file

## Running tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Why MAX_CONCURRENT=4?

Google starts returning CAPTCHAs at roughly 10-20 queries/minute from the same IP. With 5 upstream engines per SearXNG query, 4 concurrent queries means ~20 upstream requests in flight -- right at the threshold. Adjust based on your upstream engine configuration and IP reputation.

## License

MIT
