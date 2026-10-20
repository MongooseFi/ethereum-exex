//! Fan-out of notifications over a Unix socket, run inside the node.
//!
//! [`Server::publish`] never waits: each notification is encoded once, kept
//! in a ring of the last [`RING`] notifications and offered to every client's
//! bounded queue. A client whose queue is full is dropped; it reconnects and
//! resumes, or resyncs from its own checkpoint. The node is never slowed by a
//! consumer.
//!
//! A client's first frame is [`Frame::Hello`]. With `resume_from` set to a
//! unambiguous head still in the ring (bootstrap), or an exact stream cursor
//! (reconnect), the notifications after it are replayed before
//! live ones; replay and registration happen under one lock, so nothing is
//! skipped or duplicated. An unknown head gets [`Frame::Resync`] first.
use crate::wire::{
    encode, Cursor, Decoder, Frame, Notification, PendingTransaction, WireError, VERSION,
};
use alloy_primitives::B256;
use std::{
    collections::VecDeque,
    io,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::mpsc,
    task::JoinHandle,
};

/// Includes a finalized bootstrap anchor plus the head distance and cold-start
/// allowance. A 64-entry ring cannot even resume from a head 64 blocks behind.
pub const RING: usize = 256;
/// Count alone is insufficient because a v4 notification may approach 64 MiB.
pub const REPLAY_BYTES: usize = 128 * 1024 * 1024;
/// Frames a client may lag behind before it is dropped.
pub const CLIENT_QUEUE: usize = 64;
/// Time a new connection has to send its `Hello`.
pub const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

type Encoded = Arc<Vec<u8>>;

struct Client {
    sender: mpsc::Sender<Encoded>,
    pending: bool,
}

struct State {
    closed: bool,
    stream: B256,
    sequence: u64,
    /// `(head after the notification, encoded frame)`, oldest first.
    ring: VecDeque<(Option<B256>, Cursor, Encoded)>,
    ring_bytes: usize,
    clients: Vec<Client>,
}

impl Default for State {
    fn default() -> Self {
        static INCARNATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        // Domain separated identity changes on each bind, process and restart.
        let stamp = format!(
            "mongoosefi-exex-v4:{}:{:?}:{}",
            std::process::id(),
            std::time::SystemTime::now(),
            INCARNATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        Self {
            closed: false,
            stream: alloy_primitives::keccak256(stamp.as_bytes()),
            sequence: 0,
            ring: VecDeque::new(),
            ring_bytes: 0,
            clients: Vec::new(),
        }
    }
}

#[derive(Clone, Default)]
pub struct Server {
    state: Arc<Mutex<State>>,
}

impl Server {
    /// Bind `path` (replacing a stale socket file) and accept clients until
    /// the returned task is aborted. Dropping a Tokio JoinHandle detaches it.
    pub fn bind(path: &Path) -> io::Result<(Self, JoinHandle<()>)> {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        let listener = UnixListener::bind(path)?;
        let server = Self::default();
        let accept = server.clone();
        let task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let server = accept.clone();
                        tokio::spawn(async move {
                            if let Err(error) = server.serve(stream).await {
                                tracing::debug!(%error, "exex_stream.client_closed");
                            }
                        });
                    }
                    Err(error) => {
                        tracing::warn!(%error, "exex_stream.accept_failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });
        Ok((server, task))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Interrupt connections without ending this stream; clients may resume.
    pub fn disconnect_clients(&self) {
        self.lock().clients.clear();
    }

    /// End this stream permanently. In-flight handshakes cannot re-register.
    pub fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        state.clients.clear();
        state.ring.clear();
        state.ring_bytes = 0;
    }

    /// Clients currently connected and keeping up.
    pub fn clients(&self) -> usize {
        self.lock().clients.len()
    }

    /// Offer `notification` to every client and keep it for resuming ones.
    pub fn publish(&self, notification: &Notification) -> Result<(), WireError> {
        self.publish_with_limit(notification, crate::wire::MAX_FRAME)
    }

    fn publish_with_limit(
        &self,
        notification: &Notification,
        limit: usize,
    ) -> Result<(), WireError> {
        let mut state = self.lock();
        if state.closed {
            return Err(WireError::Closed);
        }
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or(WireError::SequenceExhausted)?;
        let cursor = Cursor {
            stream: state.stream,
            sequence,
        };
        let encoded = crate::wire::encode_with_limit(
            &Frame::Notification {
                cursor,
                notification: notification.clone(),
            },
            limit,
        );
        let frame: Encoded = match encoded {
            Ok(frame) => Arc::new(frame),
            Err(error @ WireError::TooLarge(_)) => {
                // An omitted canonical update invalidates every previous replay cursor.
                // Tell live clients to rebuild and make reconnects reject the old stream.
                let resync = Arc::new(encode(&Frame::Resync {
                    reason: "canonical notification exceeds frame limit; rebuild from checkpoint"
                        .into(),
                })?);
                let clients = std::mem::take(&mut state.clients);
                *state = State::default();
                state.clients = clients
                    .into_iter()
                    .filter(|client| client.sender.try_send(resync.clone()).is_ok())
                    .collect();
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        state.sequence = sequence;
        state.ring_bytes += frame.len();
        state
            .ring
            .push_back((notification.head(), cursor, frame.clone()));
        while state.ring.len() > RING || state.ring_bytes > REPLAY_BYTES {
            let (_, _, expired) = state.ring.pop_front().expect("over-budget replay entry");
            state.ring_bytes -= expired.len();
        }
        let before = state.clients.len();
        state
            .clients
            .retain(|client| client.sender.try_send(frame.clone()).is_ok());
        let dropped = before - state.clients.len();
        if dropped > 0 {
            tracing::warn!(dropped, "exex_stream.slow_clients_dropped");
        }
        Ok(())
    }

    /// Offer pending transactions to the clients that asked for them. Never
    /// kept for resuming clients; a client whose queue is full skips this
    /// batch and stays connected. Returns how many clients skipped it.
    pub fn publish_pending(
        &self,
        transactions: Vec<PendingTransaction>,
    ) -> Result<usize, WireError> {
        if transactions.is_empty() {
            return Ok(0);
        }
        let frame: Encoded = Arc::new(encode(&Frame::Pending { transactions })?);
        let mut state = self.lock();
        if state.closed {
            return Err(WireError::Closed);
        }
        state.clients.retain(|client| !client.sender.is_closed());
        Ok(state
            .clients
            .iter()
            .filter(|client| client.pending)
            .filter(|client| client.sender.try_send(frame.clone()).is_err())
            .count())
    }

    async fn serve(&self, mut stream: UnixStream) -> Result<(), ServeError> {
        let hello = tokio::time::timeout(HELLO_TIMEOUT, read_frame(&mut stream))
            .await
            .map_err(|_| ServeError::NoHello)??;
        let Some(Frame::Hello {
            version,
            resume_from,
            cursor,
            pending,
        }) = hello
        else {
            return Err(ServeError::NoHello);
        };
        if version != VERSION {
            return Err(WireError::Version(version).into());
        }
        let (sender, mut receiver) = mpsc::channel::<Encoded>(CLIENT_QUEUE);
        let backlog = {
            let mut state = self.lock();
            if state.closed {
                return Err(WireError::Closed.into());
            }
            let position = if let Some(cursor) = cursor {
                state.ring.iter().position(|(head, held, _)| {
                    *held == cursor && (resume_from.is_none() || *head == resume_from)
                })
            } else if let Some(head) = resume_from {
                let mut positions = state
                    .ring
                    .iter()
                    .enumerate()
                    .filter(|(_, (held, _, _))| *held == Some(head));
                let first = positions.next().map(|(i, _)| i);
                // A hash can reappear after reverts. Bootstrap has no cursor,
                // so an ambiguous position must rebuild, never skip effects.
                if positions.next().is_some() {
                    None
                } else {
                    first
                }
            } else {
                None
            };
            let backlog = match position {
                Some(index) => state
                    .ring
                    .iter()
                    .skip(index + 1)
                    .map(|(_, _, frame)| frame.clone())
                    .collect(),
                None if resume_from.is_none() && cursor.is_none() => Vec::new(),
                None => vec![Arc::new(encode(&Frame::Resync {
                    reason: "replay cursor missing, ambiguous, expired or from another stream"
                        .into(),
                })?)],
            };
            state.clients.push(Client { sender, pending });
            backlog
        };
        for frame in backlog {
            stream.write_all(&frame).await?;
        }
        while let Some(frame) = receiver.recv().await {
            stream.write_all(&frame).await?;
        }
        Ok(())
    }
}

async fn read_frame(stream: &mut UnixStream) -> Result<Option<Frame>, ServeError> {
    let mut decoder = Decoder::default();
    let mut buffer = [0u8; 4096];
    loop {
        if let Some(frame) = decoder.next_frame()? {
            return Ok(Some(frame));
        }
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(None);
        }
        decoder.push(&buffer[..read]);
    }
}

#[derive(Debug, thiserror::Error)]
enum ServeError {
    #[error("client sent no Hello")]
    NoHello,
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::tests::commit;

    fn decode(bytes: &[u8]) -> Frame {
        let mut decoder = Decoder::default();
        decoder.push(bytes);
        decoder.next_frame().unwrap().unwrap()
    }

    #[tokio::test]
    async fn oversized_update_resyncs_live_clients_and_invalidates_old_cursors() {
        let server = Server::default();
        let (sender, mut receiver) = mpsc::channel(4);
        server.lock().clients.push(Client {
            sender,
            pending: true,
        });
        server.publish(&commit(1)).unwrap();
        let Frame::Notification { cursor: old, .. } = decode(&receiver.recv().await.unwrap())
        else {
            panic!("expected initial notification")
        };
        assert!(matches!(
            server.publish_with_limit(&commit(2), 32),
            Err(WireError::TooLarge(_))
        ));
        assert!(matches!(
            decode(&receiver.recv().await.unwrap()),
            Frame::Resync { .. }
        ));
        assert!(server.lock().ring.is_empty());
        assert_eq!(server.lock().ring_bytes, 0);
        server.publish(&commit(3)).unwrap();
        let Frame::Notification { cursor: next, .. } = decode(&receiver.recv().await.unwrap())
        else {
            panic!("expected notification after resync")
        };
        assert_ne!(old.stream, next.stream);
        assert_eq!(next.sequence, 1);

        let (mut client, peer) = UnixStream::pair().unwrap();
        let task = tokio::spawn(async move { server.serve(peer).await });
        client
            .write_all(
                &encode(&Frame::Hello {
                    version: VERSION,
                    resume_from: None,
                    cursor: Some(old),
                    pending: false,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), read_frame(&mut client))
                .await
                .unwrap()
                .unwrap(),
            Some(Frame::Resync { .. })
        ));
        task.abort();
    }

    #[tokio::test]
    async fn oversized_update_disconnects_clients_that_cannot_receive_resync() {
        let server = Server::default();
        let (sender, mut receiver) = mpsc::channel(1);
        server.lock().clients.push(Client {
            sender,
            pending: false,
        });
        server.publish(&commit(1)).unwrap();
        assert!(matches!(
            server.publish_with_limit(&commit(2), 32),
            Err(WireError::TooLarge(_))
        ));
        assert_eq!(server.clients(), 0);
        assert!(receiver.recv().await.is_some());
        assert!(receiver.recv().await.is_none());
        server.publish(&commit(3)).unwrap();
    }
}
