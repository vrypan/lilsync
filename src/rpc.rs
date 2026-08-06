//! RPC client and server over Noise-encrypted TCP. The server dispatches
//! requests (root, node, entry, object, peers, join, announce); the client
//! drives sync and gossip on behalf of the daemon.

use crate::endpoints::AddressBook;
use crate::group::{self, GroupState, MemberEntry};
use crate::identity::{Identity, NodeId};
use crate::protocol::{RequestMessage, ResponseMessage};
use crate::state::{Entry, FolderState, TreeNode, hex};
use crate::transport::{NoiseConnection, max_plaintext_chunk};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tokio::time::{Duration, Instant, timeout};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const OBJECT_HEADER_TIMEOUT: Duration = Duration::from_secs(30);
const OBJECT_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);

const POOL_SIZE: usize = crate::sync::MAX_CONCURRENT_FETCHES;

struct ConnPool {
    idle: Mutex<HashMap<NodeId, VecDeque<NoiseConnection>>>,
}

impl ConnPool {
    fn checkout(&self, peer: NodeId) -> Option<NoiseConnection> {
        self.idle
            .lock()
            .expect("conn pool poisoned")
            .get_mut(&peer)
            .and_then(|q| q.pop_front())
    }

    fn checkin(&self, peer: NodeId, conn: NoiseConnection) {
        let mut idle = self.idle.lock().expect("conn pool poisoned");
        let queue = idle.entry(peer).or_default();
        if queue.len() < POOL_SIZE {
            queue.push_back(conn);
        }
    }
}

impl Default for ConnPool {
    fn default() -> Self {
        Self {
            idle: Mutex::new(HashMap::new()),
        }
    }
}

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

fn next_request_id() -> u64 {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone)]
pub enum RpcEvent {
    PeerJoined {
        peer: NodeId,
    },
    Announcement {
        peer: NodeId,
        message: crate::message::GossipMessage,
        /// IP the peer's connection was observed from; combined with the
        /// peer's advertised listen port to learn a routable endpoint.
        source_ip: std::net::IpAddr,
    },
}

#[derive(Clone)]
pub struct RpcClient {
    identity: Arc<Identity>,
    addresses: AddressBook,
    pool: Arc<ConnPool>,
}

impl RpcClient {
    pub fn new(identity: Arc<Identity>, addresses: AddressBook) -> Self {
        Self {
            identity,
            addresses,
            pool: Arc::new(ConnPool::default()),
        }
    }

    pub async fn get_root(&self, peer: NodeId) -> io::Result<([u8; 32], [u8; 32], u64)> {
        let request_id = next_request_id();
        match self
            .round_trip(peer, RequestMessage::GetRoot { request_id })
            .await?
        {
            ResponseMessage::Root {
                request_id: actual,
                state_root,
                live_root,
                lamport,
            } if actual == request_id => Ok((state_root, live_root, lamport)),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected GetRoot response from {peer}: {response:?}"
            ))),
        }
    }

    pub async fn get_peers(&self, peer: NodeId) -> io::Result<Vec<MemberEntry>> {
        let request_id = next_request_id();
        match self
            .round_trip(peer, RequestMessage::GetPeers { request_id })
            .await?
        {
            ResponseMessage::Peers {
                request_id: actual,
                members,
            } if actual == request_id => Ok(members),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected GetPeers response from {peer}: {response:?}"
            ))),
        }
    }

    pub async fn get_node(&self, peer: NodeId, prefix: &str) -> io::Result<Option<TreeNode>> {
        let request_id = next_request_id();
        let request = RequestMessage::GetNode {
            request_id,
            prefix: prefix.to_string(),
        };
        match self.round_trip(peer, request).await? {
            ResponseMessage::Node {
                request_id: actual,
                node,
            } if actual == request_id => Ok(node),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected GetNode response from {peer}: {response:?}"
            ))),
        }
    }

    pub async fn get_entries(&self, peer: NodeId, prefix: &str) -> io::Result<Vec<Entry>> {
        let request_id = next_request_id();
        let request = RequestMessage::GetEntries {
            request_id,
            prefix: prefix.to_string(),
        };
        match self.round_trip(peer, request).await? {
            ResponseMessage::Entries {
                request_id: actual,
                entries,
            } if actual == request_id => Ok(entries),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected GetEntries response from {peer}: {response:?}"
            ))),
        }
    }

    pub async fn get_object_to_file(
        &self,
        peer: NodeId,
        content_hash: [u8; 32],
        expected_size: u64,
        dest: &Path,
    ) -> io::Result<()> {
        let request_id = next_request_id();
        let request = RequestMessage::GetObject {
            request_id,
            content_hash,
        };
        let mut conn = if let Some(pooled) = self.pool.checkout(peer) {
            pooled
        } else {
            self.connect(peer).await?
        };
        conn.send_json(&request).await?;

        let header: ResponseMessage = rpc_timeout(
            OBJECT_HEADER_TIMEOUT,
            "read object header",
            peer,
            conn.recv_json(),
        )
        .await?;
        let size = match header {
            ResponseMessage::ObjectHeader {
                request_id: actual,
                size,
            } if actual == request_id => size,
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => return Err(io::Error::other(message)),
            response => {
                return Err(io::Error::other(format!(
                    "unexpected GetObject response from {peer}: {response:?}"
                )));
            }
        };
        if size != expected_size {
            return Err(io::Error::other(format!(
                "object size mismatch from {peer}: expected {expected_size}, got {size}"
            )));
        }

        let result = stream_to_file(peer, &mut conn, size, content_hash, dest).await;
        match &result {
            Ok(_) => self.pool.checkin(peer, conn),
            Err(_) => {
                let _ = tokio::fs::remove_file(dest).await;
            }
        }
        result
    }

    pub async fn join_group(
        &self,
        peer: NodeId,
        secret: String,
        joiner_id: NodeId,
    ) -> io::Result<Vec<MemberEntry>> {
        let request_id = next_request_id();
        let request = RequestMessage::Join {
            request_id,
            secret,
            joiner_id: joiner_id.to_string(),
        };
        match self.round_trip(peer, request).await? {
            ResponseMessage::JoinAccepted {
                request_id: actual,
                members,
            } if actual == request_id => Ok(members),
            ResponseMessage::JoinRejected {
                request_id: actual,
                reason,
            } if actual == request_id => Err(io::Error::other(reason)),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected Join response from {peer}: {response:?}"
            ))),
        }
    }

    pub async fn announce(
        &self,
        peer: NodeId,
        message: crate::message::GossipMessage,
    ) -> io::Result<()> {
        let request_id = next_request_id();
        match self
            .round_trip(
                peer,
                RequestMessage::Announce {
                    request_id,
                    message,
                },
            )
            .await?
        {
            ResponseMessage::Ok { request_id: actual } if actual == request_id => Ok(()),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected Announce response from {peer}: {response:?}"
            ))),
        }
    }

    async fn round_trip(
        &self,
        peer: NodeId,
        request: RequestMessage,
    ) -> io::Result<ResponseMessage> {
        // Try a pooled connection first. If it's stale (peer restarted),
        // discard it and fall through to a fresh connection.
        if let Some(mut conn) = self.pool.checkout(peer) {
            let result = rpc_timeout(REQUEST_TIMEOUT, "RPC request", peer, async {
                conn.send_json(&request).await?;
                conn.recv_json().await
            })
            .await;
            match result {
                Ok(response) => {
                    self.pool.checkin(peer, conn);
                    return Ok(response);
                }
                Err(_) => tracing::debug!("pooled connection to {peer} stale, retrying"),
            }
        }

        let mut conn = self.connect(peer).await?;
        let result = rpc_timeout(REQUEST_TIMEOUT, "RPC request", peer, async {
            conn.send_json(&request).await?;
            conn.recv_json().await
        })
        .await;
        if result.is_ok() {
            self.pool.checkin(peer, conn);
        }
        result
    }

    async fn connect(&self, peer: NodeId) -> io::Result<NoiseConnection> {
        let endpoints = self.wait_for_endpoints(peer).await?;
        let mut last_err = io::Error::other(format!("no endpoints known for peer {peer}"));
        for endpoint in &endpoints {
            let conn = rpc_timeout(
                CONNECT_TIMEOUT,
                "connect",
                peer,
                NoiseConnection::connect(endpoint, &self.identity),
            )
            .await;
            match conn {
                Ok(conn) if conn.peer() == peer => return Ok(conn),
                Ok(conn) => {
                    last_err = io::Error::other(format!(
                        "connected to {endpoint}, expected {peer}, got {}",
                        conn.peer()
                    ));
                }
                Err(err) => {
                    tracing::debug!("connect peer={peer} endpoint={endpoint} failed: {err}");
                    last_err = err;
                }
            }
        }
        Err(last_err)
    }

    async fn wait_for_endpoints(&self, peer: NodeId) -> io::Result<Vec<String>> {
        let deadline = Instant::now() + DISCOVERY_TIMEOUT;
        loop {
            let endpoints = self
                .addresses
                .read()
                .await
                .get(&peer)
                .map(|info| info.endpoints.clone())
                .unwrap_or_default();
            if !endpoints.is_empty() {
                return Ok(endpoints);
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("no known address for peer {peer}"),
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

pub async fn spawn_server(
    identity: Arc<Identity>,
    state: Arc<RwLock<FolderState>>,
    group: Arc<RwLock<GroupState>>,
    invites_path: PathBuf,
    events: mpsc::UnboundedSender<RpcEvent>,
    port: u16,
) -> io::Result<(u16, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await.map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("could not bind RPC listener on port {port}: {err}"),
        )
    })?;
    let port = listener.local_addr()?.port();
    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, addr)) = listener.accept().await else {
                continue;
            };
            let identity = Arc::clone(&identity);
            let state = Arc::clone(&state);
            let group = Arc::clone(&group);
            let invites_path = invites_path.clone();
            let events = events.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    handle_connection(stream, addr, identity, state, group, invites_path, events)
                        .await
                {
                    if matches!(
                        err.kind(),
                        io::ErrorKind::BrokenPipe
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                    ) {
                        tracing::debug!("rpc connection from {addr} closed: {err}");
                    } else {
                        tracing::warn!("rpc connection from {addr} failed: {err}");
                    }
                }
            });
        }
    });
    Ok((port, handle))
}

async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    identity: Arc<Identity>,
    state: Arc<RwLock<FolderState>>,
    group: Arc<RwLock<GroupState>>,
    invites_path: PathBuf,
    events: mpsc::UnboundedSender<RpcEvent>,
) -> io::Result<()> {
    let mut conn = NoiseConnection::accept(stream, &identity).await?;
    let peer = conn.peer();
    tracing::debug!("incoming rpc peer={peer} addr={addr}");
    loop {
        let request: RequestMessage = match conn.recv_json().await {
            Ok(r) => r,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };
        if let RequestMessage::GetObject {
            request_id,
            content_hash,
        } = request
        {
            handle_get_object(&mut conn, request_id, content_hash, peer, &state, &group).await?;
        } else {
            let response = handle_request(
                request,
                peer,
                addr.ip(),
                &state,
                &group,
                &invites_path,
                &events,
            )
            .await;
            conn.send_json(&response).await?;
        }
    }
    Ok(())
}

async fn handle_get_object(
    conn: &mut NoiseConnection,
    request_id: u64,
    content_hash: [u8; 32],
    peer: NodeId,
    state: &Arc<RwLock<FolderState>>,
    group: &Arc<RwLock<GroupState>>,
) -> io::Result<()> {
    if !group.read().await.is_active_member(&peer) {
        conn.send_json(&ResponseMessage::Error {
            request_id,
            message: "peer is not a group member".to_string(),
        })
        .await?;
        return Ok(());
    }

    let object = state.read().await.object_file(content_hash);
    let Some((path, expected_size)) = object else {
        conn.send_json(&ResponseMessage::Error {
            request_id,
            message: format!("object {} not found", hex(content_hash)),
        })
        .await?;
        return Ok(());
    };

    let mut file = tokio::fs::File::open(&path)
        .await
        .map_err(|err| io::Error::other(format!("open {}: {err}", path.display())))?;
    let size = file.metadata().await?.len();
    if size != expected_size {
        conn.send_json(&ResponseMessage::Error {
            request_id,
            message: format!(
                "object {} changed before transfer: expected {expected_size} bytes, got {size}",
                hex(content_hash)
            ),
        })
        .await?;
        return Ok(());
    }
    conn.send_json(&ResponseMessage::ObjectHeader { request_id, size })
        .await?;
    let mut buf = vec![0_u8; max_plaintext_chunk()];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        conn.send_plain_chunk(&buf[..n]).await?;
    }
    Ok(())
}

async fn check_member(
    group: &Arc<RwLock<GroupState>>,
    peer: NodeId,
    request_id: u64,
) -> Result<(), ResponseMessage> {
    if group.read().await.is_active_member(&peer) {
        Ok(())
    } else {
        Err(ResponseMessage::Error {
            request_id,
            message: "peer is not a group member".to_string(),
        })
    }
}

async fn check_known_member(
    group: &Arc<RwLock<GroupState>>,
    peer: NodeId,
    request_id: u64,
) -> Result<(), ResponseMessage> {
    if group.read().await.is_known_member(&peer) {
        Ok(())
    } else {
        Err(ResponseMessage::Error {
            request_id,
            message: "peer is not a group member".to_string(),
        })
    }
}

async fn handle_request(
    request: RequestMessage,
    peer: NodeId,
    source_ip: std::net::IpAddr,
    state: &Arc<RwLock<FolderState>>,
    group: &Arc<RwLock<GroupState>>,
    invites_path: &Path,
    events: &mpsc::UnboundedSender<RpcEvent>,
) -> ResponseMessage {
    match request {
        RequestMessage::Join {
            request_id,
            secret,
            joiner_id,
        } => {
            if joiner_id != peer.to_string() {
                return ResponseMessage::JoinRejected {
                    request_id,
                    reason: "joiner id does not match RPC peer".to_string(),
                };
            }
            match group::consume_invite(invites_path, &secret) {
                Ok(true) => {
                    let mut group = group.write().await;
                    match group.add_active_peer(peer) {
                        Ok(_) => {
                            let _ = events.send(RpcEvent::PeerJoined { peer });
                            ResponseMessage::JoinAccepted {
                                request_id,
                                members: group.members(),
                            }
                        }
                        Err(err) => ResponseMessage::JoinRejected {
                            request_id,
                            reason: format!("could not add peer: {err}"),
                        },
                    }
                }
                Ok(false) => ResponseMessage::JoinRejected {
                    request_id,
                    reason: "invalid or expired ticket".to_string(),
                },
                Err(err) => ResponseMessage::JoinRejected {
                    request_id,
                    reason: format!("could not validate ticket: {err}"),
                },
            }
        }
        RequestMessage::GetRoot { request_id } => {
            if let Err(e) = check_member(group, peer, request_id).await {
                return e;
            }
            let state = state.read().await;
            ResponseMessage::Root {
                request_id,
                state_root: state.root_hash(),
                live_root: state.live_root_hash(),
                lamport: state.lamport(),
            }
        }
        RequestMessage::GetPeers { request_id } => {
            if let Err(e) = check_known_member(group, peer, request_id).await {
                return e;
            }
            ResponseMessage::Peers {
                request_id,
                members: group.read().await.members(),
            }
        }
        RequestMessage::GetNode { request_id, prefix } => {
            if let Err(e) = check_member(group, peer, request_id).await {
                return e;
            }
            let state = state.read().await;
            ResponseMessage::Node {
                request_id,
                node: state.node(&prefix),
            }
        }
        RequestMessage::GetEntries { request_id, prefix } => {
            if let Err(e) = check_member(group, peer, request_id).await {
                return e;
            }
            let state = state.read().await;
            let entries = match state.node(&prefix) {
                None => Vec::new(),
                Some(node) => node
                    .entries
                    .keys()
                    .filter_map(|name| {
                        let prefix = prefix.trim_matches('/');
                        let path = if prefix.is_empty() {
                            name.clone()
                        } else {
                            format!("{prefix}/{name}")
                        };
                        state.entry(&path)
                    })
                    .collect(),
            };
            ResponseMessage::Entries {
                request_id,
                entries,
            }
        }
        RequestMessage::Announce {
            request_id,
            message,
        } => {
            if let Err(e) = check_member(group, peer, request_id).await {
                return e;
            }
            let _ = events.send(RpcEvent::Announcement {
                peer,
                message,
                source_ip,
            });
            ResponseMessage::Ok { request_id }
        }
        RequestMessage::GetObject { .. } => unreachable!("handled before handle_request"),
    }
}

async fn stream_to_file(
    peer: NodeId,
    conn: &mut NoiseConnection,
    size: u64,
    expected_hash: [u8; 32],
    dest: &Path,
) -> io::Result<()> {
    let mut file = tokio::fs::File::create(dest).await?;
    let mut hasher = blake3::Hasher::new();
    let mut remaining = size;
    let mut buf = vec![0u8; max_plaintext_chunk()];

    while remaining > 0 {
        let to_read = (remaining as usize).min(buf.len());
        rpc_timeout(
            OBJECT_READ_IDLE_TIMEOUT,
            "object transfer",
            peer,
            conn.recv_plain_exact(&mut buf[..to_read]),
        )
        .await?;
        hasher.update(&buf[..to_read]);
        file.write_all(&buf[..to_read]).await?;
        remaining -= to_read as u64;
    }

    file.sync_all().await?;

    let actual_hash = *hasher.finalize().as_bytes();
    if actual_hash != expected_hash {
        return Err(io::Error::other(format!(
            "object hash mismatch: expected {}, got {}",
            hex(expected_hash),
            hex(actual_hash),
        )));
    }

    Ok(())
}

async fn rpc_timeout<T>(
    duration: Duration,
    operation: &str,
    peer: NodeId,
    future: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    timeout(duration, future).await.map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "{operation} timed out after {}s for peer {peer}",
                duration.as_secs()
            ),
        )
    })?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rpc_timeout_returns_timed_out_error() {
        let peer = NodeId::from_bytes([9; 32]);
        let err = rpc_timeout(Duration::from_millis(1), "test operation", peer, async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        })
        .await
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(err.to_string().contains("test operation timed out"));
    }
}
