# syntax=docker/dockerfile:1.7
#
# Multi-stage build producing three minimal images from one source tree:
#   docker build -f deploy/docker/Dockerfile --target api     -t eventflow-api .
#   docker build -f deploy/docker/Dockerfile --target worker  -t eventflow-worker .
#   docker build -f deploy/docker/Dockerfile --target migrate -t eventflow-migrate .
#
# Runtime images are distroless (no shell, no package manager) and run as an
# unprivileged user.

ARG GO_VERSION=1.24
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod go.sum ./
# Optional: behind a TLS-intercepting corporate proxy, pass its CA bundle as a
# build secret (never baked into a layer):
#   docker build --secret id=extra_ca,src=/path/to/ca-bundle.crt ...
RUN --mount=type=secret,id=extra_ca,required=false \
    --mount=type=cache,target=/go/pkg/mod \
    if [ -s /run/secrets/extra_ca ]; then export SSL_CERT_FILE=/run/secrets/extra_ca; fi; \
    go mod download && go mod verify
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w -X main.version=${VERSION}" -o /out/api ./cmd/api && \
    go build -ldflags="-s -w -X main.version=${VERSION}" -o /out/worker ./cmd/worker && \
    go build -ldflags="-s -w" -o /out/migrate ./cmd/migrate && \
    go build -ldflags="-s -w" -o /out/healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static-debian12:nonroot AS runtime-base
COPY --from=build /out/healthcheck /usr/local/bin/healthcheck
USER nonroot:nonroot

FROM runtime-base AS api
COPY --from=build /out/api /usr/local/bin/eventflow-api
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=4s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/healthcheck", "http://127.0.0.1:8080/health"]
ENTRYPOINT ["/usr/local/bin/eventflow-api"]

FROM runtime-base AS worker
COPY --from=build /out/worker /usr/local/bin/eventflow-worker
EXPOSE 9091
HEALTHCHECK --interval=10s --timeout=4s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/healthcheck", "http://127.0.0.1:9091/health"]
ENTRYPOINT ["/usr/local/bin/eventflow-worker"]

FROM runtime-base AS migrate
COPY --from=build /out/migrate /usr/local/bin/eventflow-migrate
ENTRYPOINT ["/usr/local/bin/eventflow-migrate"]
CMD ["up"]
