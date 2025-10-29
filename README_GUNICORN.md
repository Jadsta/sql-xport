Quick guide: running sql_exporter with Gunicorn inside Docker and optional TLS

Overview
- The repo's Dockerfile builds an image that runs Gunicorn (see `gunicorn_config.py`).
- For development or simple setups you can run Gunicorn with TLS by mounting cert/key into the container and overriding the command to include `--certfile/--keyfile`.
- For production, prefer terminating TLS at a reverse proxy (nginx, Traefik) and run Gunicorn bound to localhost.

Create self-signed certs (for testing only)

# On Linux / WSL / macOS
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout certs/server.key -out certs/server.crt -subj "/CN=localhost"

# On PowerShell (if OpenSSL available)
# same command can be used inside WSL

Build and run with docker-compose

# Build image and start container, mapping ./certs into the container
docker-compose build --no-cache
docker-compose up -d

Notes
- The provided `docker-compose.yml` mounts `./certs` to `/certs` inside the container and the compose file overrides the container command to start Gunicorn on port 8443 with the cert and key at `/certs/server.crt` and `/certs/server.key`.
- If you prefer TLS termination outside the container (production recommended), remove the `command` override from `docker-compose.yml` and instead configure your reverse proxy to forward traffic to `127.0.0.1:8000`.

Caveats
- `teradatasql` may require vendor-provided binaries or additional system packages; if your Docker image needs it, install per the Teradata Python driver docs.
- Self-signed certs will be rejected by browsers and some clients; include `-k` with curl for testing.

Example test (from host)

# Allow insecure (self-signed) TLS for testing
curl -k https://localhost:8443/metrics?exporter=your-exporter

Further options
- If you want automatic HTTPS with Let's Encrypt in Docker, consider using a reverse-proxy companion like Traefik (lets-encrypt built-in) or nginx + certbot.
- If you want me to add an nginx sidecar Compose service and a minimal TLS-termination configuration, I can add that next.
