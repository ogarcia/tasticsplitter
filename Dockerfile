# syntax=docker/dockerfile:1.7
#
# Two-stage build. The builder compiles against musl in alpine; the
# runtime ships just the static binary on top of a minimal alpine.
# The result is a ~15-20 MB image with no glibc or OpenSSL runtime
# dependency — every C bit (libsqlite3, etc.) is bundled and every
# TLS bit (rustls) is pure Rust.
#
# Pinning notes:
#   * Alpine is pinned to 3.23 in both stages so a new base image
#     never silently changes the runtime.
#   * Rust is left floating within the 1.x channel, mirroring the
#     `dtolnay/rust-toolchain@stable` posture of the CI workflow.
#     Switch to `rust:1.<N>-alpine3.23` if bit-exact reproducibility
#     of the toolchain is required.

FROM rust:1-alpine3.23 AS builder

# musl-dev pulls in the C toolchain bits libsqlite3-sys needs to
# bundle SQLite; the rest of the stack is pure Rust.
RUN apk add --no-cache musl-dev

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked && \
    cp target/release/tasticsplitter /usr/bin/tasticsplitter && \
    strip /usr/bin/tasticsplitter

# ----------------------------------------------------------------------

FROM alpine:3.23

RUN addgroup -S -g 10001 tasticsplitter && \
    adduser -S -G tasticsplitter -u 10001 tasticsplitter && \
    mkdir -p /etc/tasticsplitter /var/lib/tasticsplitter && \
    chown -R tasticsplitter:tasticsplitter /var/lib/tasticsplitter

COPY --from=builder /usr/bin/tasticsplitter /usr/bin/tasticsplitter

USER tasticsplitter
WORKDIR /var/lib/tasticsplitter

EXPOSE 4403

ENV TASTICSPLITTER_DATA_DIR=/var/lib/tasticsplitter

ENTRYPOINT ["/usr/bin/tasticsplitter"]
