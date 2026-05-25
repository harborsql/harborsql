FROM debian:bookworm-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 10001 harborsql \
    && useradd --uid 10001 --gid harborsql --home-dir /nonexistent --shell /usr/sbin/nologin --no-create-home harborsql

COPY harborsql /usr/local/bin/harborsql

USER harborsql
ENTRYPOINT ["/usr/local/bin/harborsql"]
CMD ["server"]
