# syntax=docker/dockerfile:1.7
#
# Local build (compiles from source):
#   podman build -t tasticsplitter:dev .
#
# CI build (binary pre-compiled into artifacts/<arch>/, arch = amd64|arm64 to
# match Docker's TARGETARCH; buildx picks the right one per platform):
#   podman build --build-arg BUILDER=prebuilt .
#
# Either way the result is a static musl binary on top of a minimal alpine,
# with no glibc or OpenSSL runtime dependency: SQLite is bundled.

# ARG before the first FROM so it is a global build argument. Default 'builder'
# (local compile); CI passes --build-arg BUILDER=prebuilt to use artifacts/.
ARG BUILDER=builder

# Base-image versions. Defaults mirror versions.env (the single source of
# truth); CI overrides them with --build-arg.
ARG ALPINE_VERSION=3.23
ARG RUST_VERSION=1

# ── Stage 1a: compile from source (default local path) ──────────────────────

FROM docker.io/rust:${RUST_VERSION}-alpine${ALPINE_VERSION} AS builder

# musl-dev pulls in the C toolchain bits libsqlite3-sys needs to bundle
# SQLite; the rest of the stack is pure Rust.
RUN apk add --no-cache musl-dev

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked && \
    cp target/release/tasticsplitter /usr/bin/tasticsplitter && \
    strip /usr/bin/tasticsplitter

# ── Stage 1b: pre-built binary injected from CI (artifacts/ in context) ──────

# TARGETARCH (amd64/arm64) is set automatically by buildx per target platform;
# re-declare it here to use it in COPY. The artifacts/<arch>/ layout is produced
# by the container.yml build matrix.
FROM scratch AS prebuilt
ARG TARGETARCH
COPY --chmod=0755 artifacts/${TARGETARCH}/tasticsplitter /usr/bin/tasticsplitter

# ── Stage 1c: indirection — points to 'builder' or 'prebuilt' via build arg ──

FROM ${BUILDER} AS bins

# ── Stage 2: runtime ────────────────────────────────────────────────────────

FROM docker.io/alpine:${ALPINE_VERSION} AS runtime

RUN addgroup -S -g 10001 tasticsplitter && \
    adduser -S -G tasticsplitter -u 10001 tasticsplitter && \
    mkdir -p /etc/tasticsplitter /var/lib/tasticsplitter && \
    chown -R tasticsplitter:tasticsplitter /var/lib/tasticsplitter

COPY --from=bins /usr/bin/tasticsplitter /usr/bin/tasticsplitter

USER tasticsplitter
WORKDIR /var/lib/tasticsplitter

EXPOSE 4403

ENV TASTICSPLITTER_DATA_DIR=/var/lib/tasticsplitter

ENTRYPOINT ["/usr/bin/tasticsplitter"]
