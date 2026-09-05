# ---- build ----
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./go.mod
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /redis-server ./cmd/redis-server

# ---- runtime ----
FROM alpine:3.20
RUN addgroup -S redis && adduser -S redis -G redis \
    && mkdir -p /data && chown redis:redis /data
COPY --from=build /redis-server /usr/local/bin/redis-server
USER redis
VOLUME /data
EXPOSE 6379
ENTRYPOINT ["redis-server", "--host", "0.0.0.0", "--port", "6379", "--dir", "/data", "--appendonly", "--appendfsync", "everysec", "--save", "60", "--save-changes", "1"]
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s --retries=3 \
    CMD ["sh", "-c", "printf '*1\\r\\n$4\\r\\nPING\\r\\n' | nc -w 2 127.0.0.1 6379 | grep -q PONG"]
