//! reth 2.4.1 with the MongooseFi thin ExEx.
//!
//! The command line, datadir and behaviour are stock reth (`mongoosefi-reth
//! node ...` takes the same flags as `reth node ...`); the only addition is
//! one ExEx that publishes every chain update on a Unix socket
//! (`MONGOOSEFI_EXEX_SOCKET`, default `mongoosefi-exex.sock` in the working
//! directory) through `exex_stream::server`.
//!
//! Pending transactions: every transaction entering the node's pending pool
//! is offered, in batches, to the clients that asked for them (best effort,
//! never replayed).
//!
//! The ExEx starts at the node's head: no history is replayed. It reports
//! each committed tip as finished right after queueing it, so it never holds
//! back the node's pruning or WAL; consumers that fall behind are dropped and
//! resync from their own checkpoints.
mod convert;

use alloy_consensus::Transaction as _;
use alloy_eips::eip2718::Encodable2718;
use exex_stream::{server::Server, wire::PendingTransaction};
use futures::StreamExt;
use futures::TryStreamExt;
use reth_ethereum::pool::{NewTransactionEvent, PoolTransaction, TransactionPool};
use reth_ethereum::{
    exex::{ExExContext, ExExEvent},
    node::{api::FullNodeComponents, builder::NodeHandleFor, EthereumNode},
    EthPrimitives,
};
use std::path::PathBuf;
use tracing::{info, warn};

async fn exex<Node>(mut ctx: ExExContext<Node>, socket: PathBuf) -> eyre::Result<()>
where
    Node:
        FullNodeComponents<Types: reth_ethereum::node::api::NodeTypes<Primitives = EthPrimitives>>,
{
    let (server, accept) = Server::bind(&socket)?;
    let _lifetime = StreamLifetime {
        server: server.clone(),
        accept,
    };
    info!(socket = %socket.display(), head = ?ctx.head, "mongoosefi.exex_listening");
    let _pending = AbortOnDrop(tokio::spawn(stream_pending(
        ctx.pool().clone(),
        server.clone(),
    )));
    while let Some(notification) = ctx.notifications.try_next().await? {
        let message = convert::notification(&notification)?;
        server.publish(&message)?;
        if let Some(committed) = notification.committed_chain() {
            ctx.events
                .send(ExExEvent::FinishedHeight(committed.tip().num_hash()))?;
        }
    }
    Ok(())
}

struct StreamLifetime {
    server: Server,
    accept: tokio::task::JoinHandle<()>,
}
impl Drop for StreamLifetime {
    fn drop(&mut self) {
        self.server.close();
        self.accept.abort();
    }
}

// The allocator, signal handling and launch mirror stock reth's `main`
// (bin/reth/src/main.rs at the pinned commit); only the ExEx is added.
#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

#[cfg(all(feature = "jemalloc", unix))]
use reth_cli_util::allocator::tikv_jemalloc_sys as _;

/// Largest pending batch per frame.
const PENDING_BATCH: usize = 256;

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Forward the pool's new pending transactions: wait for one, then take
/// whatever else is already queued, up to [`PENDING_BATCH`].
async fn stream_pending<P>(pool: P, server: Server)
where
    P: TransactionPool,
    P::Transaction: PoolTransaction<Consensus = reth_ethereum::TransactionSigned>,
{
    let mut events = pool.new_pending_pool_transactions_listener();
    let pending = |event: NewTransactionEvent<P::Transaction>| {
        let transaction = &event.transaction.transaction;
        PendingTransaction {
            hash: *transaction.hash(),
            sender: transaction.sender(),
            nonce: transaction.nonce(),
            to: transaction.to(),
            raw: transaction
                .clone_into_consensus()
                .into_inner()
                .encoded_2718()
                .into(),
        }
    };
    while let Some(first) = events.next().await {
        let mut batch = vec![pending(first)];
        while batch.len() < PENDING_BATCH {
            match events.try_recv() {
                Ok(event) => batch.push(pending(event)),
                Err(_) => break,
            }
        }
        match server.publish_pending(batch) {
            Ok(skipped) if skipped > 0 => {
                tracing::debug!(skipped, "mongoosefi.exex_pending_skipped");
            }
            Ok(_) => {}
            Err(error) => warn!(%error, "mongoosefi.exex_pending_publish_failed"),
        }
    }
}

fn main() {
    #[cfg(feature = "jit")]
    {
        match reth_node_ethereum::node::maybe_run_jit_helper() {
            Ok(std::ops::ControlFlow::Break(())) => return,
            Ok(std::ops::ControlFlow::Continue(())) => {}
            Err(err) => {
                eprintln!("Error: {err:?}");
                std::process::exit(1);
            }
        }
    }

    reth_cli_util::sigsegv_handler::install();

    if std::env::var_os("RUST_BACKTRACE").is_none() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    use clap::Parser;
    if let Err(err) =
        reth::cli::Cli::<reth_ethereum_cli::chainspec::EthereumChainSpecParser>::parse().run(
            async move |builder, _| {
                info!(target: "reth::cli", "Launching node");
                let socket = std::env::var_os("MONGOOSEFI_EXEX_SOCKET")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("mongoosefi-exex.sock"));
                let handle: NodeHandleFor<EthereumNode> = builder
                    .node(EthereumNode::default())
                    .install_exex("mongoosefi", async move |ctx| Ok(exex(ctx, socket)))
                    .launch_with_debug_capabilities()
                    .await?;
                handle.wait_for_node_exit().await
            },
        )
    {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
