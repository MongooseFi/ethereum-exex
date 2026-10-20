# MongooseFi Ethereum ExEx

Ethereum execution node and the versioned data interface consumed by
[MongooseFi Services](https://github.com/MongooseFi/services).
`mongoosefi-reth` embeds a thin ExEx in Reth: the same process provides Ethereum
JSON-RPC and a Unix socket carrying canonical updates, state changes and pending
transactions. Current deployment and operational validation target Ethereum
mainnet; the binary retains Reth's Ethereum chain configuration parser.

| Package | Responsibility |
| --- | --- |
| `mongoosefi-reth` (`exex/`) | Reth integration, notification conversion and pending pool feed |
| `exex-stream` (`crates/exex_stream/`) | Wire version 4, framing, replay, socket server and client; no Reth dependency |

Reth is pinned to `8eb210175687c9f0c889a3b6795c16781d830e3a` (2.4.1).
The binary name, CLI, socket environment variable and wire version are unchanged
by the repository split. [Source provenance](docs/source-migration.json) records
the original Services commit and hashes of every migrated Rust source file.

## Build

Use the Rust toolchain in `rust-toolchain.toml`. On Ubuntu, install `build-essential`,
`pkg-config`, `libssl-dev`, `clang`, `libclang-dev`, `cmake` and `libgmp-dev`.

```sh
cargo build --locked --release -p mongoosefi-reth
cargo run --locked -p exex-stream --example tail -- /absolute/feed.sock --pending
```

For a node built with JIT, install the pinned Reth revision's LLVM 22 prerequisites
and add `--features jit`. Keep the deployed binary's feature set when upgrading.
See [ExEx semantics and node operations](docs/exex.md). Building and publishing
an artifact do not install it or restart a node.

## Services dependency

Services consumes only `exex-stream`, using a full Git commit in its `Cargo.toml`
and `Cargo.lock`. Node releases do not update Services automatically. Keep this
crate free of Reth and trading business dependencies. An incompatible wire change
must increment `wire::VERSION`; deploy a compatible producer and consumer
explicitly. Version mismatches fail closed.

This repository is public. Services fetches the pinned protocol commit over HTTPS;
no node repository credential, SSH agent, sibling checkout or submodule is required.

## CI and releases

`ci.yml` runs formatting and the existing stream checks. Pull requests and manual
CI also call `build-binaries.yml`, the same reusable native build used by releases.
Services has its own CI and does not compile the Reth node.

Push a semantic version tag beginning with `v` to release, for example:

```sh
git tag -a v0.1.0 -m "release: v0.1.0"
git push origin v0.1.0
```

The release has three ordered stages:

1. **Build**: `build-binaries.yml` produces `mongoosefi-reth-linux-amd64` and
   `mongoosefi-reth-linux-arm64` on native Ubuntu runners, once per architecture.
   Each artifact records its source commit, version, libraries and SHA-256.
2. **Package**: after both builds succeed, native image jobs download those
   binaries. The Dockerfile contains no Rust compiler. Each image must run
   `--version` and contain a byte-identical copy of its release binary before push.
3. **Publish**: merge the two image digests into
   `ghcr.io/mongoosefi/ethereum-exex:<tag>` and publish the two raw binaries,
   `SHA256SUMS` and the image manifest as GitHub Release assets.

Images and binaries use default features including jemalloc, without JIT.
The runtime image is Ubuntu 24.04, matching the build platform's glibc generation.
CI and releases never install artifacts on Z790, restart a node or run trading
strategies. `scripts/check-exex-live.py` remains an optional, manually invoked
read-only observation tool.

The repository split preserves every migrated Rust source file. Source equality,
locked dependency resolution, formatting and focused Services boundary checks
cover the migration; the tag workflow verifies the actual release artifacts.
