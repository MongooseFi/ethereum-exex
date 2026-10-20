//! reth notifications to `exex_stream` messages.
use exex_stream::wire::{
    AccountChange, AccountInfo, Block, BlockRef, Log, Notification, Segment, StateDiff,
};
use reth_ethereum::{
    exex::ExExNotification,
    primitives::{AlloyBlockHeader, RecoveredBlock},
    provider::Chain,
    EthPrimitives,
};

pub fn notification(notification: &ExExNotification<EthPrimitives>) -> eyre::Result<Notification> {
    Ok(match notification {
        ExExNotification::ChainCommitted { new } => Notification::Commit {
            committed: segment(new)?,
        },
        ExExNotification::ChainReverted { old } => Notification::Revert {
            reverted: newest_first(old),
        },
        ExExNotification::ChainReorged { old, new } => Notification::Reorg {
            reverted: newest_first(old),
            committed: segment(new)?,
        },
    })
}

fn reference(block: &RecoveredBlock<reth_ethereum::Block>) -> BlockRef {
    BlockRef {
        number: block.header().number(),
        hash: block.hash(),
        parent_hash: block.header().parent_hash(),
    }
}

fn newest_first(chain: &Chain<EthPrimitives>) -> Vec<BlockRef> {
    let mut blocks: Vec<BlockRef> = chain.blocks_iter().map(|b| reference(b)).collect();
    blocks.reverse();
    blocks
}

fn segment(chain: &Chain<EthPrimitives>) -> eyre::Result<Segment> {
    let blocks = chain
        .blocks_iter()
        .map(|block| {
            let header = block.header();
            let hash = block.hash();
            let mut logs = Vec::new();
            let receipts = chain
                .receipts_by_block_hash(hash)
                .ok_or_else(|| eyre::eyre!("missing receipts for {hash}"))?;
            eyre::ensure!(
                receipts.len() == block.body().transactions.len(),
                "receipt/transaction count mismatch"
            );
            for (transaction_index, receipt) in receipts.iter().enumerate() {
                for log in &receipt.logs {
                    logs.push(Log {
                        address: log.address,
                        topics: log.topics().to_vec(),
                        data: log.data.data.clone(),
                        log_index: logs.len() as u64,
                        transaction_index: transaction_index as u64,
                        transaction_hash: *block.body().transactions[transaction_index].tx_hash(),
                    });
                }
            }
            Ok(Block {
                number: header.number(),
                hash,
                parent_hash: header.parent_hash(),
                timestamp: header.timestamp(),
                base_fee: header.base_fee_per_gas(),
                gas_limit: header.gas_limit(),
                gas_used: header.gas_used(),
                blob_gas_used: header.blob_gas_used(),
                beneficiary: header.beneficiary(),
                prev_randao: header.mix_hash().unwrap_or_default(),
                excess_blob_gas: header.excess_blob_gas(),
                transactions: block
                    .body()
                    .transactions
                    .iter()
                    .map(|tx| *tx.tx_hash())
                    .collect(),
                logs,
            })
        })
        .collect::<eyre::Result<Vec<_>>>()?;
    let bundle = &chain.execution_outcome().bundle;
    let accounts = bundle
        .state
        .iter()
        .map(|(address, account)| {
            let original_code = account.original_info.as_ref().map(|info| info.code_hash);
            let info = account.info.as_ref().map(|info| AccountInfo {
                balance: info.balance,
                nonce: info.nonce,
                code_hash: info.code_hash,
                code: (original_code != Some(info.code_hash))
                    .then(|| {
                        bundle
                            .contracts
                            .get(&info.code_hash)
                            .map(|code| code.original_bytes())
                    })
                    .flatten(),
            });
            let mut storage: Vec<_> = account
                .storage
                .iter()
                .filter(|(_, slot)| slot.is_changed())
                .map(|(key, slot)| (*key, slot.present_value()))
                .collect();
            storage.sort_unstable();
            AccountChange {
                address: *address,
                info,
                storage_wiped: account.was_destroyed(),
                storage,
            }
        })
        .collect();
    Ok(Segment {
        blocks,
        diff: StateDiff { accounts },
    })
}
