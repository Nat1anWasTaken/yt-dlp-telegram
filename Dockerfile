FROM rust:1.85-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates curl python3 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp \
    -o /usr/local/bin/yt-dlp \
    && chmod +x /usr/local/bin/yt-dlp

RUN useradd -m -u 10001 app
WORKDIR /app

COPY --from=builder /app/target/release/yt-dlp-telegram /usr/local/bin/yt-dlp-telegram

USER app
ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/yt-dlp-telegram"]
