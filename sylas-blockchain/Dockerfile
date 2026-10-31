FROM rust:1.85-slim-bookworm AS build

WORKDIR /src
COPY Cargo.toml ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/sylas --shell /usr/sbin/nologin sylas \
    && mkdir -p /var/lib/sylas \
    && chown -R sylas:sylas /var/lib/sylas

COPY --from=build /src/target/release/sylas-blockchain /usr/local/bin/sylas-blockchain

USER sylas
ENV SYLAS_HTTP_BIND=0.0.0.0:5000
ENV SYLAS_TX_BIND=0.0.0.0:7077
ENV SYLAS_P2P_BIND=0.0.0.0:9090
ENV SYLAS_PUBLIC_P2P_ADDR=
ENV SYLAS_NODE_ID=
ENV SYLAS_BOOTSTRAP_PEERS=
ENV SYLAS_VALIDATORS=
ENV SYLAS_STATE_PATH=/var/lib/sylas/state.mmap
ENV SYLAS_LEDGER_PATH=/var/lib/sylas/ledger.dat
ENV SYLAS_RECEIPTS_PATH=/var/lib/sylas/receipts.log
ENV SYLAS_STATE_CAPACITY=65536
ENV SYLAS_MAX_BATCH=1024
ENV SYLAS_GENESIS_ACCOUNTS=

EXPOSE 5000 7077 9090
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s CMD wget -qO- http://127.0.0.1:5000/healthz || exit 1

CMD ["sylas-blockchain"]
