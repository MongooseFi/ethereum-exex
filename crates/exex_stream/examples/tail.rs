//! Print a one-line summary of every notification on an ExEx socket.
//!
//! ```text
//! cargo run -p exex-stream --example tail -- /path/to/mongoosefi-exex.sock [--pending]
//! ```
use exex_stream::{
    client::Connection,
    wire::{Frame, Notification},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).ok_or("usage: tail <socket>")?;
    let pending = std::env::args().any(|arg| arg == "--pending");
    let mut connection = Connection::connect_with(path.as_ref(), None, None, pending).await?;
    while let Some(frame) = connection.next().await? {
        match frame {
            Frame::Notification { notification, .. } => {
                let (kind, reverted, committed) = match &notification {
                    Notification::Commit { committed } => ("commit", 0, Some(committed)),
                    Notification::Revert { reverted } => ("revert", reverted.len(), None),
                    Notification::Reorg {
                        reverted,
                        committed,
                    } => ("reorg", reverted.len(), Some(committed)),
                };
                let (blocks, txs, logs, accounts, slots) =
                    committed.map_or((0, 0, 0, 0, 0), |segment| {
                        (
                            segment.blocks.len(),
                            segment
                                .blocks
                                .iter()
                                .map(|b| b.transactions.len())
                                .sum::<usize>(),
                            segment.blocks.iter().map(|b| b.logs.len()).sum::<usize>(),
                            segment.diff.accounts.len(),
                            segment
                                .diff
                                .accounts
                                .iter()
                                .map(|a| a.storage.len())
                                .sum::<usize>(),
                        )
                    });
                let tip = notification.tip().map(|tip| tip.number);
                println!(
                    "{kind} tip={tip:?} head={:?} reverted={reverted} blocks={blocks} txs={txs} logs={logs} accounts={accounts} slots={slots}",
                    notification.head()
                );
            }
            Frame::Pending { transactions } => {
                let first = transactions.first().map(|tx| tx.hash);
                println!("pending count={} first={first:?}", transactions.len());
            }
            Frame::Resync { reason } => println!("resync: {reason}"),
            Frame::Hello { .. } => {}
        }
    }
    println!("closed");
    Ok(())
}
