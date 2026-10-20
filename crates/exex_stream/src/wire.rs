//! Messages between the node's ExEx and its consumers, and their framing.
//!
//! Every frame is a 4-byte big-endian length followed by that many bytes of
//! JSON. JSON keeps frames readable with standard tools; a block's frame is
//! typically well under a megabyte and [`MAX_FRAME`] bounds the worst case.
use alloy_primitives::{Address, Bytes, B256, U256};
use serde::{Deserialize, Serialize};

/// Bumped on any incompatible change; a peer with another version is refused.
pub const VERSION: u32 = 4;

/// Largest accepted frame body.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

/// Exact replay position, scoped to one server incarnation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub stream: B256,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    /// Client to server, first frame: the wire version and, when resuming,
    /// the hash of the last applied block and its exact stream cursor.
    /// A hash alone is used only for bootstrap and must resolve unambiguously.
    Hello {
        version: u32,
        resume_from: Option<B256>,
        cursor: Option<Cursor>,
        /// Also send the node's pending transactions ([`Frame::Pending`]).
        #[serde(default)]
        pending: bool,
    },
    /// Server to client.
    Notification {
        cursor: Cursor,
        notification: Notification,
    },
    /// Server to client, only when the `Hello` asked for them: transactions
    /// that entered the node's pending pool. Delivered best effort: never
    /// replayed, and skipped rather than queued when the client lags.
    Pending {
        transactions: Vec<PendingTransaction>,
    },
    /// Server to client: the cursor expired/changed stream, or the bootstrap
    /// head is missing or ambiguous.
    /// The client must rebuild its state from its own checkpoint, then take
    /// the notifications that follow this frame.
    Resync { reason: String },
}

/// One chain update, as the node commits it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Notification {
    Commit {
        committed: Segment,
    },
    /// Blocks removed from the tip, newest first.
    Revert {
        reverted: Vec<BlockRef>,
    },
    Reorg {
        /// Newest first, as in `Revert`.
        reverted: Vec<BlockRef>,
        committed: Segment,
    },
}

impl Notification {
    /// The block the chain ends at after this notification, when it commits.
    pub fn tip(&self) -> Option<BlockRef> {
        match self {
            Self::Commit { committed } | Self::Reorg { committed, .. } => {
                committed.blocks.last().map(Block::reference)
            }
            Self::Revert { .. } => None,
        }
    }

    /// The new chain head after this notification: the committed tip, or for
    /// a pure revert the parent of the oldest reverted block.
    pub fn head(&self) -> Option<B256> {
        match self {
            Self::Revert { reverted } => reverted.last().map(|block| block.parent_hash),
            other => other.tip().map(|block| block.hash),
        }
    }
}

/// Consecutive committed blocks and their combined state changes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    /// Oldest first.
    pub blocks: Vec<Block>,
    /// Every account and slot the segment changed, with its value after the
    /// last block. The node aggregates a segment's changes; intermediate
    /// blocks have no state of their own here.
    pub diff: StateDiff,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRef {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
}

/// A block's environment and all its logs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: u64,
    pub base_fee: Option<u64>,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub blob_gas_used: Option<u64>,
    pub beneficiary: Address,
    pub prev_randao: B256,
    pub excess_blob_gas: Option<u64>,
    /// Transaction hashes, in block order.
    pub transactions: Vec<B256>,
    /// In block order.
    pub logs: Vec<Log>,
}

impl Block {
    pub fn reference(&self) -> BlockRef {
        BlockRef {
            number: self.number,
            hash: self.hash,
            parent_hash: self.parent_hash,
        }
    }
}

/// A transaction that entered the node's pending pool.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTransaction {
    pub hash: B256,
    /// Recovered by the node's pool.
    pub sender: Address,
    pub nonce: u64,
    pub to: Option<Address>,
    /// The signed transaction, EIP-2718 encoded.
    pub raw: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Log {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
    /// Position among all the block's logs, as receipts number them.
    pub log_index: u64,
    pub transaction_index: u64,
    pub transaction_hash: B256,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateDiff {
    pub accounts: Vec<AccountChange>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountChange {
    pub address: Address,
    /// The account after the segment; `None` when it no longer exists.
    pub info: Option<AccountInfo>,
    /// The account's storage was cleared (destroyed or re-created) before the
    /// listed slots were written; every other slot is now zero.
    pub storage_wiped: bool,
    /// Changed slots and their new values.
    pub storage: Vec<(U256, U256)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountInfo {
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: B256,
    /// Present when the segment deployed or changed the code.
    pub code: Option<Bytes>,
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("ExEx stream closed")]
    Closed,
    #[error("frame of {0} bytes exceeds the {MAX_FRAME}-byte limit")]
    TooLarge(usize),
    #[error("malformed frame: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("peer speaks wire version {0}, this side {VERSION}")]
    Version(u32),
    #[error("ExEx sequence exhausted")]
    SequenceExhausted,
}

/// A frame with its length prefix, ready to write.
pub fn encode(frame: &Frame) -> Result<Vec<u8>, WireError> {
    encode_with_limit(frame, MAX_FRAME)
}

/// Stop serialization at the limit, rather than allocating a complete oversized JSON body.
pub(crate) fn encode_with_limit(frame: &Frame, limit: usize) -> Result<Vec<u8>, WireError> {
    struct Bounded {
        bytes: Vec<u8>,
        limit: usize,
        exceeded: Option<usize>,
    }
    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let length = self.bytes.len() - 4 + bytes.len();
            if length > self.limit {
                self.exceeded = Some(length);
                return Err(std::io::Error::other("ExEx frame size limit exceeded"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Bounded {
        bytes: vec![0; 4],
        limit,
        exceeded: None,
    };
    let result = serde_json::to_writer(&mut writer, frame);
    if let Some(length) = writer.exceeded {
        return Err(WireError::TooLarge(length));
    }
    result?;
    let length = (writer.bytes.len() - 4) as u32;
    writer.bytes[..4].copy_from_slice(&length.to_be_bytes());
    Ok(writer.bytes)
}

/// Incremental decoder: push bytes as they arrive, pop whole frames.
#[derive(Debug, Default)]
pub struct Decoder {
    buffer: Vec<u8>,
}

impl Decoder {
    pub fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    pub fn next_frame(&mut self) -> Result<Option<Frame>, WireError> {
        let Some(prefix) = self.buffer.get(..4) else {
            return Ok(None);
        };
        let length = u32::from_be_bytes(prefix.try_into().expect("four bytes")) as usize;
        if length > MAX_FRAME {
            return Err(WireError::TooLarge(length));
        }
        if self.buffer.len() < 4 + length {
            return Ok(None);
        }
        let frame = serde_json::from_slice(&self.buffer[4..4 + length])?;
        self.buffer.drain(..4 + length);
        Ok(Some(frame))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn block(number: u64, parent: B256) -> Block {
        Block {
            number,
            hash: B256::with_last_byte(number as u8),
            parent_hash: parent,
            timestamp: 1_700_000_000 + number * 12,
            base_fee: Some(1_000_000_000),
            gas_limit: 60_000_000,
            gas_used: 21_000,
            blob_gas_used: Some(0),
            beneficiary: Address::repeat_byte(0xcb),
            prev_randao: B256::repeat_byte(2),
            excess_blob_gas: Some(0),
            transactions: vec![B256::repeat_byte(7)],
            logs: vec![Log {
                address: Address::repeat_byte(1),
                topics: vec![B256::repeat_byte(3)],
                data: Bytes::from_static(b"\x01\x02"),
                log_index: 0,
                transaction_index: 0,
                transaction_hash: B256::repeat_byte(7),
            }],
        }
    }

    pub(crate) fn commit(number: u64) -> Notification {
        Notification::Commit {
            committed: Segment {
                blocks: vec![block(number, B256::with_last_byte(number as u8 - 1))],
                diff: StateDiff {
                    accounts: vec![AccountChange {
                        address: Address::repeat_byte(1),
                        info: Some(AccountInfo {
                            balance: U256::from(5),
                            nonce: 1,
                            code_hash: B256::repeat_byte(4),
                            code: None,
                        }),
                        storage_wiped: false,
                        storage: vec![(U256::from(1), U256::from(number))],
                    }],
                },
            },
        }
    }

    #[test]
    fn frames_round_trip_through_partial_reads() {
        let frames = vec![
            Frame::Hello {
                version: VERSION,
                resume_from: Some(B256::repeat_byte(9)),
                cursor: None,
                pending: true,
            },
            Frame::Pending {
                transactions: vec![PendingTransaction {
                    hash: B256::repeat_byte(1),
                    sender: Address::repeat_byte(2),
                    nonce: 3,
                    to: None,
                    raw: Bytes::from_static(b"\x02\x01"),
                }],
            },
            Frame::Notification {
                cursor: Cursor {
                    stream: B256::ZERO,
                    sequence: 1,
                },
                notification: commit(5),
            },
            Frame::Notification {
                cursor: Cursor {
                    stream: B256::ZERO,
                    sequence: 2,
                },
                notification: Notification::Revert {
                    reverted: vec![block(5, B256::with_last_byte(4)).reference()],
                },
            },
            Frame::Resync {
                reason: "gone".into(),
            },
        ];
        let bytes: Vec<u8> = frames
            .iter()
            .flat_map(|frame| encode(frame).unwrap())
            .collect();
        let mut decoder = Decoder::default();
        let mut decoded = Vec::new();
        // One byte at a time: every split point must work.
        for byte in bytes {
            decoder.push(&[byte]);
            while let Some(frame) = decoder.next_frame().unwrap() {
                decoded.push(frame);
            }
        }
        assert_eq!(decoded, frames);
    }

    #[test]
    fn encoding_enforces_the_exact_body_limit() {
        let frame = Frame::Resync {
            reason: "x".repeat(1024),
        };
        let body = serde_json::to_vec(&frame).unwrap();
        let encoded = encode_with_limit(&frame, body.len()).unwrap();
        assert_eq!(&encoded[4..], body);
        assert_eq!(&encoded[..4], &(body.len() as u32).to_be_bytes());
        assert!(matches!(
            encode_with_limit(&frame, body.len() - 1),
            Err(WireError::TooLarge(_))
        ));
        assert!(matches!(
            encode_with_limit(&frame, 32),
            Err(WireError::TooLarge(_))
        ));
    }

    #[test]
    fn oversized_frames_are_refused_before_buffering_them() {
        let mut decoder = Decoder::default();
        decoder.push(&((MAX_FRAME as u32) + 1).to_be_bytes());
        assert!(matches!(decoder.next_frame(), Err(WireError::TooLarge(_))));
    }

    #[test]
    fn heads_follow_commits_and_reverts() {
        assert_eq!(commit(5).head(), Some(B256::with_last_byte(5)));
        let revert = Notification::Revert {
            reverted: vec![
                block(6, B256::with_last_byte(5)).reference(),
                block(5, B256::with_last_byte(4)).reference(),
            ],
        };
        assert_eq!(revert.tip(), None);
        assert_eq!(revert.head(), Some(B256::with_last_byte(4)));
    }
}
