# syntax=docker/dockerfile:1

# ---- build stage: pure-Go binaries (CGO disabled thanks to modernc sqlite) ----
FROM golang:1.27-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly
# Unit tests gate the image: a failing code test fails the image build, so
# the one-shot verify flow's exit code covers tests + build + API smoke.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go test -count=1 ./... && \
    go build -trimpath -ldflags="-s -w" -o /out/server  ./cmd/server && \
    go build -trimpath -ldflags="-s -w" -o /out/verify  ./cmd/verify

# ---- runtime ----
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 app \
 && useradd  --system --uid 10001 --gid app --home /home/app --create-home app \
 && mkdir -p /data /shared \
 && chown -R app:app /data /shared

COPY --from=build /out/server /out/verify /usr/local/bin/

USER app
WORKDIR /home/app
EXPOSE 8080

# In-image health probe; no shell/curl dependency.
HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=20 \
    CMD ["/usr/local/bin/server", "-healthcheck"]

CMD ["/usr/local/bin/server"]
