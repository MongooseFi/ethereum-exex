//! Consumer side: connect, say `Hello`, read frames. Reconnection and what
//! to do on [`Frame::Resync`] belong to the consumer.
use crate::wire::{
    encode, Cursor, Decoder, Frame, Notification, PendingTransaction, WireError, VERSION,
};
use alloy_primitives::B256;
use std::{io, path::Path};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error("ExEx replay sequence discontinuity")]
    Gap,
}

pub struct Connection {
    stream: UnixStream,
    decoder: Decoder,
    buffer: Box<[u8; 64 * 1024]>,
    cursor: Option<Cursor>,
}

impl Connection {
    /// Connect to `path`, resuming after head `resume_from` when given.
    pub async fn connect(path: &Path, resume_from: Option<B256>) -> Result<Self, ClientError> {
        Self::connect_with(path, resume_from, None, false).await
    }

    /// As [`Self::connect`]; with `pending`, also receive pending transactions.
    pub async fn connect_with(
        path: &Path,
        resume_from: Option<B256>,
        cursor: Option<Cursor>,
        pending: bool,
    ) -> Result<Self, ClientError> {
        let mut stream = UnixStream::connect(path).await?;
        stream
            .write_all(&encode(&Frame::Hello {
                version: VERSION,
                resume_from,
                cursor,
                pending,
            })?)
            .await?;
        Ok(Self {
            stream,
            decoder: Decoder::default(),
            buffer: Box::new([0u8; 64 * 1024]),
            cursor,
        })
    }

    /// The next frame, or `None` when the server closed the connection.
    pub async fn next(&mut self) -> Result<Option<Frame>, ClientError> {
        loop {
            if let Some(frame) = self.decoder.next_frame()? {
                if let Frame::Notification { cursor, .. } = &frame {
                    if cursor.sequence == 0
                        || self.cursor.is_some_and(|previous| {
                            previous.stream != cursor.stream
                                || previous.sequence.checked_add(1) != Some(cursor.sequence)
                        })
                    {
                        return Err(ClientError::Gap);
                    }
                    self.cursor = Some(*cursor);
                }
                return Ok(Some(frame));
            }
            let read = self.stream.read(&mut self.buffer[..]).await?;
            if read == 0 {
                return Ok(None);
            }
            self.decoder.push(&self.buffer[..read]);
        }
    }
}

const RECONNECT_MIN: std::time::Duration = std::time::Duration::from_millis(250);
const RECONNECT_MAX: std::time::Duration = std::time::Duration::from_secs(30);

/// What a follower receives.
pub enum Event<'a> {
    Notification(&'a Notification),
    Pending(&'a [PendingTransaction]),
    /// Replay continuity is lost. Consumers must invalidate admission/rebuild.
    Resync(&'a str),
}

/// Follow normalized events until the consumer returns false. Replay gaps are
/// delivered explicitly; no wrapper may hide them from state admission.
pub fn follow_events<F>(
    path: std::path::PathBuf,
    pending: bool,
    mut on_event: F,
) -> tokio::task::JoinHandle<()>
where
    F: FnMut(Event<'_>) -> bool + Send + 'static,
{
    tokio::spawn(async move {
        let mut last_head = None;
        let mut cursor = None;
        let mut delay = RECONNECT_MIN;
        loop {
            match Connection::connect_with(&path, last_head, cursor, pending).await {
                Ok(mut connection) => {
                    tracing::info!(path = %path.display(), ?last_head, "exex_stream.connected");
                    delay = RECONNECT_MIN;
                    loop {
                        match connection.next().await {
                            Ok(Some(Frame::Notification {
                                cursor: position,
                                notification,
                            })) => {
                                cursor = Some(position);
                                if let Some(head) = notification.head() {
                                    last_head = Some(head);
                                }
                                if !on_event(Event::Notification(&notification)) {
                                    return;
                                }
                            }
                            Ok(Some(Frame::Pending { transactions })) => {
                                if !on_event(Event::Pending(&transactions)) {
                                    return;
                                }
                            }
                            Ok(Some(Frame::Resync { reason })) => {
                                tracing::warn!(%reason, "exex_stream.resync");
                                last_head = None;
                                cursor = None;
                                if !on_event(Event::Resync(&reason)) {
                                    return;
                                }
                                break;
                            }
                            Ok(Some(Frame::Hello { .. })) => {}
                            Ok(None) => break,
                            Err(ClientError::Gap) => {
                                if !on_event(Event::Resync("ExEx sequence gap or wrong stream")) {
                                    return;
                                }
                                last_head = None;
                                cursor = None;
                                break;
                            }
                            Err(error) => {
                                tracing::warn!(%error, "exex_stream.read_failed");
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(%error, path = %path.display(), "exex_stream.unavailable");
                }
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(RECONNECT_MAX);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        server::{Server, CLIENT_QUEUE, RING},
        wire::{tests::commit, Notification},
    };
    use std::time::Duration;

    fn socket(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("exex-stream-{}-{name}.sock", std::process::id()))
    }

    async fn expect(connection: &mut Connection) -> Frame {
        tokio::time::timeout(Duration::from_secs(5), connection.next())
            .await
            .expect("frame in time")
            .unwrap()
            .expect("open connection")
    }

    fn notification(frame: Frame) -> Notification {
        let Frame::Notification { notification, .. } = frame else {
            panic!("expected notification")
        };
        notification
    }

    async fn wait_for_clients(server: &Server, n: usize) {
        for _ in 0..200 {
            if server.clients() == n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("expected {n} clients, have {}", server.clients());
    }

    #[tokio::test]
    async fn live_notifications_reach_every_client() {
        let path = socket("live");
        let (server, _task) = Server::bind(&path).unwrap();
        let mut a = Connection::connect(&path, None).await.unwrap();
        let mut b = Connection::connect(&path, None).await.unwrap();
        wait_for_clients(&server, 2).await;
        for n in 1..=3 {
            server.publish(&commit(n)).unwrap();
        }
        for connection in [&mut a, &mut b] {
            for n in 1..=3 {
                assert_eq!(notification(expect(connection).await), commit(n));
            }
        }
    }

    #[tokio::test]
    async fn a_resuming_client_gets_what_it_missed_then_live_ones() {
        let path = socket("resume");
        let (server, _task) = Server::bind(&path).unwrap();
        for n in 1..=3 {
            server.publish(&commit(n)).unwrap();
        }
        let resume = commit(1).head();
        let mut connection = Connection::connect(&path, resume).await.unwrap();
        wait_for_clients(&server, 1).await;
        server.publish(&commit(4)).unwrap();
        for n in 2..=4 {
            assert_eq!(notification(expect(&mut connection).await), commit(n));
        }
    }

    #[tokio::test]
    async fn an_evicted_or_unknown_head_is_told_to_resync() {
        let path = socket("resync");
        let (server, _task) = Server::bind(&path).unwrap();
        server.publish(&commit(2)).unwrap();
        for n in 1..=(RING as u64 + 5) {
            server.publish(&commit(n % 200 + 10)).unwrap();
        }
        let evicted = commit(2).head();
        let mut connection = Connection::connect(&path, evicted).await.unwrap();
        assert!(matches!(
            expect(&mut connection).await,
            Frame::Resync { .. }
        ));
        wait_for_clients(&server, 1).await;
        server.publish(&commit(9)).unwrap();
        assert_eq!(notification(expect(&mut connection).await), commit(9));
    }

    #[tokio::test]
    async fn finalized_bootstrap_replays_more_than_sixty_four_blocks() {
        let path = socket("finalized-bootstrap");
        let (server, task) = Server::bind(&path).unwrap();
        for n in 1..=100 {
            server.publish(&commit(n)).unwrap();
        }
        let mut connection = Connection::connect(&path, commit(1).head()).await.unwrap();
        for n in 2..=100 {
            assert_eq!(notification(expect(&mut connection).await), commit(n));
        }
        server.close();
        task.abort();
    }

    #[tokio::test]
    async fn a_client_that_stops_reading_is_dropped_without_blocking() {
        let path = socket("slow");
        let (server, _task) = Server::bind(&path).unwrap();
        let _stalled = Connection::connect(&path, None).await.unwrap();
        wait_for_clients(&server, 1).await;
        // Large frames fill the socket buffer, then the client's queue.
        let mut big = commit(1);
        if let Notification::Commit { committed } = &mut big {
            committed.blocks[0].logs[0].data = vec![7u8; 256 * 1024].into();
        }
        let started = std::time::Instant::now();
        for _ in 0..(CLIENT_QUEUE * 4) {
            server.publish(&big).unwrap();
            tokio::task::yield_now().await;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "publish blocked"
        );
        assert_eq!(server.clients(), 0);
    }

    #[tokio::test]
    async fn closing_a_stream_rejects_existing_and_late_clients_and_publication() {
        let path = socket("closed");
        let (server, accept) = Server::bind(&path).unwrap();
        let mut first = Connection::connect(&path, None).await.unwrap();
        wait_for_clients(&server, 1).await;
        server.close();
        assert!(server.publish(&commit(1)).is_err());
        assert!(server.publish_pending(vec![pending(1)]).is_err());
        assert!(tokio::time::timeout(Duration::from_secs(2), first.next())
            .await
            .unwrap()
            .unwrap()
            .is_none());
        // Keep the listener alive to exercise a handshake racing shutdown.
        let mut late = Connection::connect(&path, None).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(2), late.next())
            .await
            .unwrap()
            .unwrap()
            .is_none());
        assert_eq!(server.clients(), 0);
        accept.abort();
    }

    #[tokio::test]
    async fn a_follower_reconnects_and_resumes_within_the_same_stream() {
        let path = socket("follow");
        let (server, _accept) = Server::bind(&path).unwrap();
        let (sender, mut received) = tokio::sync::mpsc::unbounded_channel();
        let follower = follow_events(path.clone(), false, move |event| {
            let Event::Notification(notification) = event else {
                return false;
            };
            sender.send(notification.clone()).is_ok()
        });
        wait_for_clients(&server, 1).await;
        server.publish(&commit(1)).unwrap();
        assert_eq!(received.recv().await.unwrap(), commit(1));

        // A dropped connection resumes by its exact cursor in this stream.
        // A new server incarnation instead requires a rebuild (tested below).
        server.disconnect_clients();
        server.publish(&commit(2)).unwrap();
        let next = tokio::time::timeout(Duration::from_secs(10), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next, commit(2));
        follower.abort();
    }

    fn pending(n: u8) -> crate::wire::PendingTransaction {
        crate::wire::PendingTransaction {
            hash: alloy_primitives::B256::repeat_byte(n),
            sender: alloy_primitives::Address::repeat_byte(n),
            nonce: n.into(),
            to: None,
            raw: alloy_primitives::Bytes::from(vec![2, n]),
        }
    }

    #[tokio::test]
    async fn pending_transactions_reach_only_clients_that_asked() {
        let path = socket("pending");
        let (server, _task) = Server::bind(&path).unwrap();
        let mut wants = Connection::connect_with(&path, None, None, true)
            .await
            .unwrap();
        let mut chain_only = Connection::connect(&path, None).await.unwrap();
        wait_for_clients(&server, 2).await;
        assert_eq!(server.publish_pending(vec![pending(1)]).unwrap(), 0);
        server.publish(&commit(1)).unwrap();
        assert_eq!(
            expect(&mut wants).await,
            Frame::Pending {
                transactions: vec![pending(1)]
            }
        );
        assert_eq!(notification(expect(&mut wants).await), commit(1));
        // The chain-only client never sees the pending batch.
        assert_eq!(notification(expect(&mut chain_only).await), commit(1));
        // Pending batches are not kept for resuming clients.
        let mut late = Connection::connect_with(&path, commit(1).head(), None, true)
            .await
            .unwrap();
        wait_for_clients(&server, 3).await;
        server.publish(&commit(2)).unwrap();
        assert_eq!(notification(expect(&mut late).await), commit(2));
    }

    #[tokio::test]
    async fn a_lagging_pending_client_skips_batches_but_stays_connected() {
        let path = socket("pending-lag");
        let (server, _task) = Server::bind(&path).unwrap();
        let _stalled = Connection::connect_with(&path, None, None, true)
            .await
            .unwrap();
        wait_for_clients(&server, 1).await;
        let big = crate::wire::PendingTransaction {
            raw: vec![7u8; 256 * 1024].into(),
            ..pending(1)
        };
        let mut skipped = 0;
        for _ in 0..(CLIENT_QUEUE * 4) {
            skipped += server.publish_pending(vec![big.clone()]).unwrap();
            tokio::task::yield_now().await;
        }
        assert!(skipped > 0, "the stalled client's queue never filled");
        assert_eq!(
            server.clients(),
            1,
            "skipping pending batches keeps the client"
        );
    }

    #[tokio::test]
    async fn another_wire_version_is_refused() {
        let path = socket("version");
        let (server, _task) = Server::bind(&path).unwrap();
        let mut stream = UnixStream::connect(&path).await.unwrap();
        stream
            .write_all(
                &encode(&Frame::Hello {
                    version: VERSION + 1,
                    resume_from: None,
                    cursor: None,
                    pending: false,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, 0, "server closes the connection");
        assert_eq!(server.clients(), 0);
    }
    #[tokio::test]
    async fn exact_cursor_resumes_across_repeated_heads_while_hash_is_ambiguous() {
        let path = socket("cursor-repeated-head");
        let (server, task) = Server::bind(&path).unwrap();
        let mut first = Connection::connect(&path, None).await.unwrap();
        wait_for_clients(&server, 1).await;
        server.publish(&commit(1)).unwrap();
        assert_eq!(notification(expect(&mut first).await), commit(1));
        let cursor = first.cursor;
        server.publish(&commit(2)).unwrap();
        let Notification::Commit { committed } = commit(2) else {
            unreachable!()
        };
        let revert = Notification::Revert {
            reverted: vec![committed.blocks[0].reference()],
        };
        server.publish(&revert).unwrap();
        let mut ambiguous = Connection::connect(&path, commit(1).head()).await.unwrap();
        assert!(matches!(expect(&mut ambiguous).await, Frame::Resync { .. }));
        let mut resumed = Connection::connect_with(&path, commit(1).head(), cursor, false)
            .await
            .unwrap();
        assert_eq!(notification(expect(&mut resumed).await), commit(2));
        assert_eq!(notification(expect(&mut resumed).await), revert);
        task.abort();
    }

    #[tokio::test]
    async fn server_restart_invalidates_old_cursor_even_when_head_repeats() {
        let path = socket("cursor-restart");
        let (server, task) = Server::bind(&path).unwrap();
        let mut first = Connection::connect(&path, None).await.unwrap();
        wait_for_clients(&server, 1).await;
        server.publish(&commit(1)).unwrap();
        expect(&mut first).await;
        let cursor = first.cursor;
        task.abort();
        server.disconnect_clients();
        let (next, task) = Server::bind(&path).unwrap();
        next.publish(&commit(1)).unwrap();
        let mut resumed = Connection::connect_with(&path, commit(1).head(), cursor, false)
            .await
            .unwrap();
        assert!(matches!(expect(&mut resumed).await, Frame::Resync { .. }));
        task.abort();
    }

    #[tokio::test]
    async fn sequence_gaps_duplicates_and_epoch_changes_are_rejected() {
        for (name, second) in [
            (
                "skip",
                Cursor {
                    stream: B256::ZERO,
                    sequence: 3,
                },
            ),
            (
                "duplicate",
                Cursor {
                    stream: B256::ZERO,
                    sequence: 1,
                },
            ),
            (
                "epoch",
                Cursor {
                    stream: B256::with_last_byte(1),
                    sequence: 2,
                },
            ),
        ] {
            let path = socket(name);
            let _ = std::fs::remove_file(&path);
            let listener = tokio::net::UnixListener::bind(&path).unwrap();
            let task = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let length = stream.read_u32().await.unwrap();
                let mut hello = vec![0; length as usize];
                stream.read_exact(&mut hello).await.unwrap();
                for cursor in [
                    Cursor {
                        stream: B256::ZERO,
                        sequence: 1,
                    },
                    second,
                ] {
                    stream
                        .write_all(
                            &encode(&Frame::Notification {
                                cursor,
                                notification: commit(1),
                            })
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                }
            });
            let mut connection = Connection::connect(&path, None).await.unwrap();
            assert!(connection.next().await.unwrap().is_some());
            assert!(matches!(connection.next().await, Err(ClientError::Gap)));
            task.await.unwrap();
        }
    }
}

#[cfg(test)]
mod gap_tests {
    use super::*;
    #[tokio::test]
    async fn replay_gap_is_delivered_to_the_consumer_and_can_stop_the_follower() {
        use tokio::net::UnixListener;
        let path = std::env::temp_dir().join(format!("exex-gap-{}.sock", std::process::id()));
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let length = stream.read_u32().await.unwrap() as usize;
            assert!(length <= 4096);
            let mut hello = vec![0u8; length];
            stream.read_exact(&mut hello).await.unwrap();
            assert!(matches!(
                serde_json::from_slice::<Frame>(&hello).unwrap(),
                Frame::Hello { .. }
            ));
            stream
                .write_all(
                    &encode(&Frame::Resync {
                        reason: "test gap".into(),
                    })
                    .unwrap(),
                )
                .await
                .unwrap();
        });
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut tx = Some(tx);
        let follower = follow_events(path.clone(), false, move |event| match event {
            Event::Resync(reason) => {
                tx.take().unwrap().send(reason.to_owned()).unwrap();
                false
            }
            _ => panic!("expected replay gap"),
        });
        let reason = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reason, "test gap");
        tokio::time::timeout(std::time::Duration::from_secs(5), follower)
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
