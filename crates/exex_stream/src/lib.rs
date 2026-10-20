//! The node's thin ExEx stream.
//!
//! The ExEx inside reth publishes each chain update with every block's
//! environment and logs and the exact state changes; the state feed reads
//! them over a local Unix socket instead of polling RPC.
//!
//! - [`wire`]: messages and length-prefixed JSON framing, versioned.
//! - [`server`]: fan-out inside the node; never blocks the node.
//! - [`client`]: a consumer's connection, with resume by head hash.
pub mod client;
pub mod server;
pub mod wire;
