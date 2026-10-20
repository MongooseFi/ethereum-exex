# Package binaries built by build-binaries.yml; never compile in this image.
FROM ubuntu:24.04
ARG TARGETARCH
ARG VERSION
ARG REVISION
LABEL org.opencontainers.image.source="https://github.com/MongooseFi/ethereum-exex" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libgcc-s1 libstdc++6 libgmp10 libssl3t64 \
    && rm -rf /var/lib/apt/lists/*
COPY --chmod=755 dist/mongoosefi-reth-linux-${TARGETARCH} /usr/local/bin/mongoosefi-reth
WORKDIR /data
EXPOSE 30303 30303/udp 8545 8546 8551
ENTRYPOINT ["/usr/local/bin/mongoosefi-reth"]
