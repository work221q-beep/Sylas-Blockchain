FROM rust:1.85-slim-bookworm AS build

WORKDIR /src
COPY apps/coinflip/Cargo.toml ./Cargo.toml
COPY apps/coinflip/src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/sylas-coinflip --shell /usr/sbin/nologin sylasgame \
    && mkdir -p /var/lib/sylas-coinflip \
    && chown -R sylasgame:sylasgame /var/lib/sylas-coinflip

COPY --from=build /src/target/release/sylas-coinflip /usr/local/bin/sylas-coinflip

USER sylasgame
ENV SYLAS_COINFLIP_BIND=0.0.0.0:5080
ENV SYLAS_NODE_URL=http://sylas:5000
ENV SYLAS_COINFLIP_HOUSE_SECRET_HEX=

EXPOSE 5080
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s CMD wget -qO- http://127.0.0.1:5080/healthz || exit 1

CMD ["sylas-coinflip"]
