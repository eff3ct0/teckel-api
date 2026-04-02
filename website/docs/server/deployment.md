---
sidebar_position: 4
---

# Deployment

This page covers how to deploy the Teckel server for production use.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `TECKEL_HOST` | `0.0.0.0` | Bind address for the HTTP server |
| `TECKEL_PORT` | `8080` | Port for the HTTP server |
| `TECKEL_MAX_CONCURRENCY` | Number of CPU cores | Maximum concurrent pipeline executions |
| `RUST_LOG` | `info` | Logging level (uses `tracing-subscriber` with `EnvFilter`) |

## Running Locally

```bash
# Build and run with defaults
cargo run --bin teckel-server

# With custom configuration
TECKEL_HOST=127.0.0.1 TECKEL_PORT=9090 TECKEL_MAX_CONCURRENCY=4 RUST_LOG=debug \
  cargo run --bin teckel-server
```

The server starts and logs its configuration:

```
INFO addr=0.0.0.0:8080 max_concurrency=8 teckel-server starting
```

## Docker

### Dockerfile

```dockerfile
FROM rust:1.83-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin teckel-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/teckel-server /usr/local/bin/
EXPOSE 8080
CMD ["teckel-server"]
```

### Build and Run

```bash
docker build -t teckel-server .
docker run -p 8080:8080 \
  -e TECKEL_MAX_CONCURRENCY=4 \
  -e RUST_LOG=info \
  teckel-server
```

### Data Volumes

If your pipelines read/write local files, mount the data directory:

```bash
docker run -p 8080:8080 \
  -v /path/to/data:/data \
  teckel-server
```

## Docker Compose

### With Teckel UI

```yaml
version: '3.8'

services:
  teckel-server:
    build: .
    ports:
      - "8080:8080"
    environment:
      TECKEL_HOST: "0.0.0.0"
      TECKEL_PORT: "8080"
      TECKEL_MAX_CONCURRENCY: "4"
      RUST_LOG: "info"
    volumes:
      - pipeline-data:/data

  teckel-ui:
    image: ghcr.io/eff3ct0/teckel-ui:latest
    ports:
      - "3000:3000"
    environment:
      TECKEL_API_URL: "http://teckel-server:8080"
    depends_on:
      - teckel-server

volumes:
  pipeline-data:
```

### Start the Stack

```bash
docker compose up -d
```

The Teckel UI will be available at `http://localhost:3000` and will communicate with the server at `http://localhost:8080`.

## Production Considerations

### Reverse Proxy

For production, place the server behind a reverse proxy that handles:

- **TLS termination**: The server itself does not support HTTPS.
- **Authentication**: No built-in auth; use the proxy for JWT/OAuth.
- **Rate limiting**: Prevent abuse of the job submission endpoint.

Example nginx configuration:

```nginx
server {
    listen 443 ssl;
    server_name teckel-api.example.com;

    ssl_certificate /etc/ssl/certs/teckel.crt;
    ssl_certificate_key /etc/ssl/private/teckel.key;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 600s;  # For long-polling /wait endpoint
    }
}
```

### Concurrency Tuning

The `TECKEL_MAX_CONCURRENCY` setting should match your available resources:

- **CPU-bound pipelines** (DataFusion, Polars): Set to the number of CPU cores.
- **I/O-bound pipelines** (Spark Connect, Remote): Can be set higher since the server just proxies requests.
- **Mixed workloads**: Start with CPU count and adjust based on observed resource usage.

### Logging

The server uses `tracing` with `tracing-subscriber`. Configure `RUST_LOG` for different verbosity levels:

```bash
# Production
RUST_LOG=info

# Debugging
RUST_LOG=debug

# Specific crate debugging
RUST_LOG=teckel_server=debug,teckel_engine=trace

# Quiet (errors only)
RUST_LOG=error
```

### Health Checks

Use the `/api/health` endpoint for container orchestration health checks:

```yaml
# Kubernetes liveness probe
livenessProbe:
  httpGet:
    path: /api/health
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

### Persistence

The current job store is in-memory (`DashMap`). Jobs are lost on server restart. For production deployments that need job history persistence, consider:

- Persisting job metadata to a database (PostgreSQL, SQLite)
- Using a message queue (Redis, NATS) for the job queue
- Implementing a checkpoint/resume mechanism for long-running pipelines
