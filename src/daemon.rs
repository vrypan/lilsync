//! The sync daemon: main event loop, filesystem-change handling, peer
//! announcement fanout, remote reconciliation dispatch, and GC triggering.

use crate::commands::{INVITES_FILE, KEY_FILE, PEERS_FILE, acquire_daemon_lock};
use crate::group::{GroupState, MemberEntry, MemberStatus};
use crate::identity::{Identity, NodeId};
use crate::message::{GossipMessage, TreeHint};
use crate::rpc::{self, RpcClient};
use crate::state::{Change, FolderState, GcWatermark};
use crate::sync;
use crate::ui::{
    StateSnapshot, StatusState, print_changes, print_remote_message, print_start, status_view_loop,
    summarize_message,
};
use crate::watcher;
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering as AtomicOrdering},
};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, mpsc};

const MAX_FILESYSTEM_CHANGED_BYTES: usize = 3_000;
const SYNC_STATE_HEARTBEAT_SECS: u64 = 60;
const PEERS_HEARTBEAT_SECS: u64 = 300;

#[derive(Default)]
pub struct SyncActivity {
    active: AtomicBool,
    pending_rescan: AtomicBool,
}

impl SyncActivity {
    pub fn begin(&self) {
        self.active.store(true, AtomicOrdering::SeqCst);
    }

    pub fn end(&self) {
        self.active.store(false, AtomicOrdering::SeqCst);
    }

    pub fn is_active(&self) -> bool {
        self.active.load(AtomicOrdering::SeqCst)
    }

    pub fn defer_rescan(&self) {
        self.pending_rescan.store(true, AtomicOrdering::SeqCst);
    }

    pub fn take_pending_rescan(&self) -> bool {
        self.pending_rescan.swap(false, AtomicOrdering::SeqCst)
    }
}

#[derive(Clone)]
pub struct DaemonShared {
    pub state: Arc<RwLock<FolderState>>,
    pub group: Arc<RwLock<GroupState>>,
    pub root_reports: Arc<RwLock<RootReports>>,
    pub downloads: sync::DownloadCoordinator,
    pub sync_activity: Arc<SyncActivity>,
    pub reconcile_gate: Arc<Mutex<()>>,
    pub status: Arc<StatusState>,
    pub publisher: RpcClient,
    pub local_origin: String,
}

#[derive(Clone, Eq, PartialEq)]
struct SyncPublishMarker {
    state_root: [u8; 32],
    live_root: [u8; 32],
    lamport: u64,
    gc_watermark: GcWatermark,
}

impl SyncPublishMarker {
    fn from_snapshot(snapshot: &StateSnapshot) -> Self {
        Self {
            state_root: snapshot.state_root,
            live_root: snapshot.live_root,
            lamport: snapshot.lamport,
            gc_watermark: snapshot.gc_watermark.clone(),
        }
    }
}

#[derive(Default)]
struct PublishTracker {
    last_sync: Option<SyncPublishMarker>,
    last_sync_at: Option<Instant>,
    last_peers: Option<Vec<PeerPublishMarker>>,
    last_peers_at: Option<Instant>,
}

#[derive(Clone, Eq, PartialEq)]
struct PeerPublishMarker {
    id: String,
    status: MemberStatus,
    lamport: u64,
}

impl PublishTracker {
    fn should_publish_sync(&self, snapshot: &StateSnapshot) -> bool {
        let marker = SyncPublishMarker::from_snapshot(snapshot);
        self.last_sync.as_ref() != Some(&marker)
            || self
                .last_sync_at
                .is_none_or(|at| at.elapsed() >= Duration::from_secs(SYNC_STATE_HEARTBEAT_SECS))
    }

    fn mark_sync(&mut self, snapshot: &StateSnapshot) {
        self.last_sync = Some(SyncPublishMarker::from_snapshot(snapshot));
        self.last_sync_at = Some(Instant::now());
    }

    fn should_publish_peers(&self, members: &[MemberEntry]) -> bool {
        let marker = peer_publish_marker(members);
        self.last_peers.as_ref() != Some(&marker)
            || self
                .last_peers_at
                .is_none_or(|at| at.elapsed() >= Duration::from_secs(PEERS_HEARTBEAT_SECS))
    }

    fn mark_peers(&mut self, members: &[MemberEntry]) {
        self.last_peers = Some(peer_publish_marker(members));
        self.last_peers_at = Some(Instant::now());
    }
}

fn peer_publish_marker(members: &[MemberEntry]) -> Vec<PeerPublishMarker> {
    members
        .iter()
        .map(|member| PeerPublishMarker {
            id: member.id.clone(),
            status: member.status.clone(),
            lamport: member.lamport,
        })
        .collect()
}

#[derive(Clone, Copy)]
pub struct RootReport {
    pub state_root: [u8; 32],
    pub live_root: [u8; 32],
    pub lamport: u64,
}

#[derive(Default)]
pub struct RootReports {
    reports: std::collections::BTreeMap<String, RootReport>,
}

impl RootReports {
    pub fn update(&mut self, origin: String, report: RootReport) {
        let apply = self
            .reports
            .get(&origin)
            .is_none_or(|old| report.lamport >= old.lamport);
        if apply {
            self.reports.insert(origin, report);
        }
    }

    pub fn all_active_match(&self, local_root: [u8; 32], active_peer_ids: &[String]) -> bool {
        active_peer_ids.iter().all(|id| {
            self.reports
                .get(id)
                .is_some_and(|report| report.state_root == local_root)
        })
    }

    pub fn is_synced(&self, peer: &str, local: &StateSnapshot) -> bool {
        self.reports.get(peer).is_some_and(|report| {
            report.state_root == local.state_root && report.live_root == local.live_root
        })
    }
}

pub async fn run_sync(
    folder: PathBuf,
    name: Option<String>,
    poll: bool,
    interval_ms: u64,
    announce_interval_secs: u64,
    status: bool,
) -> io::Result<()> {
    fs::create_dir_all(&folder)?;

    let state_dir = folder.join(".lil");
    fs::create_dir_all(&state_dir)?;

    let _lock = acquire_daemon_lock(&state_dir)?;

    let identity = Arc::new(Identity::load_or_create(&state_dir.join(KEY_FILE))?);
    let local_id = identity.node_id();
    let local_origin = local_id.to_string();
    let peers_path = state_dir.join(PEERS_FILE);
    let invites_path = state_dir.join(INVITES_FILE);
    let group = Arc::new(RwLock::new(GroupState::load_or_init(peers_path, local_id)?));
    let bootstrap = group.read().await.active_peers();
    let state = {
        // The initial scan reads and may hash the whole folder; keep it off
        // the async worker threads.
        let folder = folder.clone();
        let origin = local_origin.clone();
        let s = tokio::task::spawn_blocking(move || FolderState::new(folder, origin))
            .await
            .map_err(io::Error::other)??;
        Arc::new(RwLock::new(s))
    };
    let root_reports = Arc::new(RwLock::new(RootReports::default()));
    let downloads = sync::DownloadCoordinator::default();
    let sync_activity = Arc::new(SyncActivity::default());
    let reconcile_gate = Arc::new(Mutex::new(()));
    let status_state = Arc::new(StatusState::default());

    let (fs_tx, mut fs_rx) = mpsc::unbounded_channel::<Vec<PathBuf>>();
    let (rpc_event_tx, mut rpc_event_rx) = mpsc::unbounded_channel();
    let watch_root = state.read().await.root().to_path_buf();
    let _watcher = watcher::spawn(watch_root, interval_ms, poll, fs_tx)?;

    let address_book = crate::discovery::new_address_book();
    let (rpc_port, _rpc_server) = rpc::spawn_server(
        Arc::clone(&identity),
        Arc::clone(&state),
        Arc::clone(&group),
        invites_path,
        rpc_event_tx,
    )
    .await?;
    let mdns = crate::discovery::spawn(
        local_id,
        rpc_port,
        name.as_deref(),
        Arc::clone(&address_book),
    )?;
    let rpc_client = RpcClient::new(Arc::clone(&identity), Arc::clone(&address_book));
    let shared = DaemonShared {
        state: Arc::clone(&state),
        group: Arc::clone(&group),
        root_reports: Arc::clone(&root_reports),
        downloads: downloads.clone(),
        sync_activity: Arc::clone(&sync_activity),
        reconcile_gate: Arc::clone(&reconcile_gate),
        status: Arc::clone(&status_state),
        publisher: rpc_client.clone(),
        local_origin: local_origin.clone(),
    };

    {
        let state = state.read().await;
        print_start(&state, local_id, rpc_port, &bootstrap, poll, interval_ms);
    }

    {
        let state = state.read().await;
        publish_sync_state(
            &rpc_client,
            Arc::clone(&group),
            &StateSnapshot::from_state(&state),
            &local_origin,
        )
        .await;
    }
    publish_peers(&rpc_client, Arc::clone(&group), &local_origin).await;
    request_peer_lists(
        rpc_client.clone(),
        Arc::clone(&group),
        bootstrap.clone(),
        rpc_client.clone(),
        local_origin.clone(),
    );
    let mut publish_tracker = PublishTracker::default();
    {
        let state = state.read().await;
        publish_tracker.mark_sync(&StateSnapshot::from_state(&state));
    }
    {
        let group = group.read().await;
        publish_tracker.mark_peers(&group.members());
    }
    if status {
        tokio::spawn(status_view_loop(
            Arc::clone(&state),
            Arc::clone(&group),
            Arc::clone(&root_reports),
            Arc::clone(&address_book),
            Arc::clone(&status_state),
            local_origin.clone(),
        ));
    }
    maybe_gc_tombstones(
        Arc::clone(&state),
        Arc::clone(&group),
        Arc::clone(&root_reports),
        &rpc_client,
        &local_origin,
    )
    .await;

    let mut announce_interval =
        tokio::time::interval(Duration::from_secs(announce_interval_secs.max(1)));
    announce_interval.tick().await;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("shutdown signal received");
                mdns.announce_removal().await;
                return Ok(());
            }
            Some(paths) = fs_rx.recv() => {
                if sync_activity.is_active() {
                    sync_activity.defer_rescan();
                    tracing::debug!("filesystem event deferred while remote sync is active");
                    continue;
                }
                apply_filesystem_paths(
                    Arc::clone(&state),
                    &rpc_client,
                    Arc::clone(&group),
                    &local_origin,
                    paths,
                )
                .await;
            }
            Some(event) = rpc_event_rx.recv() => {
                match event {
                    rpc::RpcEvent::PeerJoined { peer } => {
                        tracing::info!("peer joined {peer}");
                        publish_peers(&rpc_client, Arc::clone(&group), &local_origin).await;
                    }
                    rpc::RpcEvent::Announcement { peer, message } => {
                        tracing::debug!("announcement from {peer}");
                        if let Err(err) = handle_announcement(peer, message, shared.clone()).await {
                            tracing::warn!("announcement handling failed for peer {peer}: {err}");
                        }
                    }
                }
            }
            _ = announce_interval.tick() => {
                let snapshot = {
                    let state = state.read().await;
                    StateSnapshot::from_state(&state)
                };
                if publish_tracker.should_publish_sync(&snapshot) {
                    publish_sync_state(&rpc_client, Arc::clone(&group), &snapshot, &local_origin).await;
                    publish_tracker.mark_sync(&snapshot);
                }
                let members = {
                    let group = group.read().await;
                    group.members()
                };
                if publish_tracker.should_publish_peers(&members) {
                    publish_peers(&rpc_client, Arc::clone(&group), &local_origin).await;
                    publish_tracker.mark_peers(&members);
                }
            }
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub async fn apply_filesystem_paths(
    state: Arc<RwLock<FolderState>>,
    rpc_client: &RpcClient,
    group: Arc<RwLock<GroupState>>,
    local_origin: &str,
    paths: Vec<PathBuf>,
) {
    let scan_start = Instant::now();
    let path_count = paths.len();
    let scan_cause = if paths.is_empty() {
        "full-rescan"
    } else {
        "event-paths"
    };
    // Scanning stats, hashes, and sleeps waiting for unstable files; run it
    // on the blocking pool so a slow file cannot stall the async workers
    // serving RPCs and announcements.
    let update = {
        let state = Arc::clone(&state);
        let local_origin = local_origin.to_string();
        tokio::task::spawn_blocking(move || {
            let mut state = state.blocking_write();
            let before_state = state.root_hash();
            let before_live = state.live_root_hash();
            let result = if paths.is_empty() {
                state.rescan()
            } else {
                state.apply_paths(paths)
            };
            match result {
                Ok(changes) if changes.is_empty() => None,
                Ok(changes) => {
                    print_changes(before_state, before_live, &state, &changes);
                    let snapshot = StateSnapshot::from_state(&state);
                    let hint = build_tree_hint(&state, &changes, &snapshot, &local_origin);
                    Some((changes, snapshot, hint))
                }
                Err(err) => {
                    tracing::warn!("scan failed: {err}");
                    None
                }
            }
        })
        .await
        .unwrap_or_else(|err| {
            tracing::warn!("scan task failed: {err}");
            None
        })
    };
    let changes_count = update
        .as_ref()
        .map(|(changes, _, _)| changes.len())
        .unwrap_or(0);
    tracing::debug!(
        "filesystem scan cause={} paths={} changes={} elapsed_ms={}",
        scan_cause,
        path_count,
        changes_count,
        scan_start.elapsed().as_millis()
    );
    if let Some((changes, snapshot, hint)) = update {
        tracing::info!("filesystem changed paths={}", changes.len());
        publish_filesystem_changed(rpc_client, group, &snapshot, local_origin, hint).await;
    }
}

async fn publish_sync_state(
    rpc_client: &RpcClient,
    group: Arc<RwLock<GroupState>>,
    state: &StateSnapshot,
    origin: &str,
) {
    let message = GossipMessage::SyncState {
        origin: origin.to_string(),
        state_root: state.state_root,
        live_root: state.live_root,
        lamport: state.lamport,
        state_nodes: state.state_nodes,
        live_nodes: state.live_nodes,
        gc_watermark: state.gc_watermark.clone(),
    };
    publish(rpc_client, group, message).await;
}

async fn publish_filesystem_changed(
    rpc_client: &RpcClient,
    group: Arc<RwLock<GroupState>>,
    state: &StateSnapshot,
    origin: &str,
    hint: Option<TreeHint>,
) {
    let message = GossipMessage::FilesystemChanged {
        origin: origin.to_string(),
        state_root: state.state_root,
        live_root: state.live_root,
        lamport: state.lamport,
        hint,
    };
    publish(rpc_client, group, message).await;
}

async fn publish_peers(rpc_client: &RpcClient, group: Arc<RwLock<GroupState>>, origin: &str) {
    let members = group.read().await.members();
    let message = GossipMessage::Peers {
        origin: origin.to_string(),
        members,
    };
    publish(rpc_client, group, message).await;
}

fn request_peer_lists(
    rpc_client: RpcClient,
    group: Arc<RwLock<GroupState>>,
    peers: Vec<NodeId>,
    publisher: RpcClient,
    local_origin: String,
) {
    for peer in peers {
        let rpc_client = rpc_client.clone();
        let group = Arc::clone(&group);
        let publisher = publisher.clone();
        let local_origin = local_origin.clone();
        tokio::spawn(async move {
            match rpc_client.get_peers(peer).await {
                Ok(members) => {
                    let update = match group.write().await.merge_members(members) {
                        Ok(update) => update,
                        Err(err) => {
                            tracing::warn!("get-peers peer={} merge failed: {err}", peer);
                            return;
                        }
                    };
                    if update.changed {
                        tracing::info!(
                            "get-peers peer={} updated active={}",
                            peer,
                            update.active_peers.len()
                        );
                        if update.removed_self {
                            tracing::warn!("this node is marked removed from the group");
                        } else {
                            publish_peers(&publisher, Arc::clone(&group), &local_origin).await;
                        }
                    }
                }
                Err(err) => {
                    tracing::debug!("get-peers peer={} failed: {err}", peer);
                }
            }
        });
    }
}

fn build_tree_hint(
    state: &FolderState,
    changes: &[Change],
    snapshot: &StateSnapshot,
    origin: &str,
) -> Option<TreeHint> {
    let mut prefixes = Vec::new();
    let mut seen = BTreeSet::new();
    push_hint_prefix(&mut prefixes, &mut seen, "");
    for change in changes {
        for prefix in path_prefixes(&change.path) {
            push_hint_prefix(&mut prefixes, &mut seen, &prefix);
        }
    }

    let mut hint = TreeHint {
        truncated: false,
        nodes: Vec::new(),
    };
    for prefix in prefixes {
        let Some(node) = state.node(&prefix) else {
            continue;
        };
        let mut candidate = hint.clone();
        candidate.nodes.push(node);
        if filesystem_changed_len(snapshot, origin, Some(&candidate))
            <= MAX_FILESYSTEM_CHANGED_BYTES
        {
            hint = candidate;
        } else {
            hint.truncated = true;
            break;
        }
    }

    if hint.nodes.is_empty() {
        None
    } else {
        Some(hint)
    }
}

fn filesystem_changed_len(
    snapshot: &StateSnapshot,
    origin: &str,
    hint: Option<&TreeHint>,
) -> usize {
    GossipMessage::FilesystemChanged {
        origin: origin.to_string(),
        state_root: snapshot.state_root,
        live_root: snapshot.live_root,
        lamport: snapshot.lamport,
        hint: hint.cloned(),
    }
    .to_bytes()
    .len()
}

fn push_hint_prefix(prefixes: &mut Vec<String>, seen: &mut BTreeSet<String>, prefix: &str) {
    let normalized = prefix.trim_matches('/').to_string();
    if seen.insert(normalized.clone()) {
        prefixes.push(normalized);
    }
}

fn path_prefixes(path: &str) -> Vec<String> {
    let parts: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|p| !p.is_empty())
        .collect();
    let mut out = Vec::new();
    for end in 1..=parts.len() {
        out.push(parts[..end].join("/"));
    }
    out
}

async fn publish(rpc_client: &RpcClient, group: Arc<RwLock<GroupState>>, message: GossipMessage) {
    tracing::debug!("announce send {}", summarize_message(&message));
    let peers = group.read().await.active_peers();
    for peer in peers {
        let rpc_client = rpc_client.clone();
        let message = message.clone();
        tokio::spawn(async move {
            if let Err(err) = rpc_client.announce(peer, message).await {
                tracing::debug!("announce peer={peer} failed: {err}");
            }
        });
    }
}

async fn handle_announcement(
    peer: NodeId,
    message: GossipMessage,
    shared: DaemonShared,
) -> io::Result<()> {
    // The announcement's self-asserted origin must match the cryptographically
    // authenticated RPC peer. Otherwise a member could forge another member's
    // origin and pollute root reports (e.g. to trigger premature tombstone GC).
    if message.origin() != peer.to_string() {
        tracing::warn!(
            "ignored announcement: origin {} does not match authenticated peer {peer}",
            message.origin()
        );
        return Ok(());
    }
    if message.origin() == shared.local_origin {
        return Ok(());
    }
    if !is_active_origin(&shared.group, message.origin()).await {
        tracing::debug!("ignored announcement from non-member {}", message.origin());
        return Ok(());
    }
    if let Some(snapshot) =
        merge_remote_gc_watermark(Arc::clone(&shared.state), message_gc_watermark(&message)).await
    {
        publish_sync_state(
            &shared.publisher,
            Arc::clone(&shared.group),
            &snapshot,
            &shared.local_origin,
        )
        .await;
    }
    let local = {
        let state = shared.state.read().await;
        StateSnapshot::from_state(&state)
    };
    print_remote_message(&message, &local);
    record_root_report(Arc::clone(&shared.root_reports), &message).await;
    match &message {
        GossipMessage::Peers { members, .. } => {
            let update = shared.group.write().await.merge_members(members.clone())?;
            if update.changed {
                tracing::info!("peers updated active={}", update.active_peers.len());
                if update.removed_self {
                    tracing::warn!("this node is marked removed from the group");
                }
            }
            maybe_gc_tombstones(
                Arc::clone(&shared.state),
                Arc::clone(&shared.group),
                Arc::clone(&shared.root_reports),
                &shared.publisher,
                &shared.local_origin,
            )
            .await;
        }
        _ => maybe_probe_remote_rpc(shared.publisher.clone(), message, shared),
    }
    Ok(())
}

async fn is_active_origin(group: &Arc<RwLock<GroupState>>, origin: &str) -> bool {
    let Ok(peer) = origin.parse::<NodeId>() else {
        return false;
    };
    group.read().await.is_active_member(&peer)
}

fn message_gc_watermark(message: &GossipMessage) -> Option<&GcWatermark> {
    match message {
        GossipMessage::SyncState { gc_watermark, .. } => Some(gc_watermark),
        GossipMessage::FilesystemChanged { .. } | GossipMessage::Peers { .. } => None,
    }
}

async fn merge_remote_gc_watermark(
    state: Arc<RwLock<FolderState>>,
    gc_watermark: Option<&GcWatermark>,
) -> Option<StateSnapshot> {
    let gc_watermark = gc_watermark?;
    if gc_watermark.is_empty() {
        return None;
    }

    let mut state = state.write().await;
    let (changed, pruned) = state.merge_gc_watermark(gc_watermark);
    if changed {
        tracing::debug!("gc watermark updated origins={}", gc_watermark.len());
    }
    if pruned == 0 {
        return None;
    }
    tracing::info!("gc watermark pruned {pruned} tombstones");
    state.save_entries();
    Some(StateSnapshot::from_state(&state))
}

async fn record_root_report(root_reports: Arc<RwLock<RootReports>>, message: &GossipMessage) {
    let Some((origin, report)) = root_report_from_message(message) else {
        return;
    };
    root_reports.write().await.update(origin, report);
}

fn root_report_from_message(message: &GossipMessage) -> Option<(String, RootReport)> {
    match message {
        GossipMessage::SyncState {
            origin,
            state_root,
            live_root,
            lamport,
            ..
        }
        | GossipMessage::FilesystemChanged {
            origin,
            state_root,
            live_root,
            lamport,
            ..
        } => Some((
            origin.clone(),
            RootReport {
                state_root: *state_root,
                live_root: *live_root,
                lamport: *lamport,
            },
        )),
        GossipMessage::Peers { .. } => None,
    }
}

pub async fn maybe_gc_tombstones(
    state: Arc<RwLock<FolderState>>,
    group: Arc<RwLock<GroupState>>,
    root_reports: Arc<RwLock<RootReports>>,
    rpc_client: &RpcClient,
    local_origin: &str,
) {
    let (local_root, active_peer_ids) = {
        let state = state.read().await;
        let group = group.read().await;
        (state.root_hash(), group.active_peer_ids())
    };
    let converged = root_reports
        .read()
        .await
        .all_active_match(local_root, &active_peer_ids);
    if !converged {
        return;
    }

    let snapshot = {
        let mut state = state.write().await;
        if state.root_hash() != local_root {
            return;
        }
        let pruned = state.gc_tombstones_for_converged_root();
        if pruned == 0 {
            return;
        }
        tracing::info!("gc converged root pruned {pruned} tombstones");
        state.save_entries();
        StateSnapshot::from_state(&state)
    };
    publish_sync_state(rpc_client, group, &snapshot, local_origin).await;
}

fn maybe_probe_remote_rpc(rpc_client: RpcClient, message: GossipMessage, shared: DaemonShared) {
    let (origin, remote_state_root, remote_live_root, remote_lamport, hint, use_advertised_root) =
        match message {
            GossipMessage::SyncState {
                origin,
                state_root,
                live_root,
                lamport,
                ..
            } => (origin, state_root, live_root, lamport, None, false),
            GossipMessage::FilesystemChanged {
                origin,
                state_root,
                live_root,
                lamport,
                hint,
                ..
            } => (origin, state_root, live_root, lamport, hint, true),
            GossipMessage::Peers { .. } => return,
        };
    let Ok(peer) = origin.parse::<NodeId>() else {
        tracing::warn!("cannot RPC probe peer with invalid origin {origin}");
        return;
    };

    tokio::spawn(async move {
        let _reconcile = shared.reconcile_gate.lock().await;
        let local = {
            let state = shared.state.read().await;
            StateSnapshot::from_state(&state)
        };

        let sync_result =
            if remote_state_root == local.state_root && remote_live_root == local.live_root {
                Ok(None)
            } else if use_advertised_root {
                shared.sync_activity.begin();
                shared.status.begin_sync(peer);
                match sync::reconcile_with_advertised_root(
                    rpc_client,
                    Arc::clone(&shared.state),
                    peer,
                    sync::RemoteRoot {
                        state_root: remote_state_root,
                        live_root: remote_live_root,
                        lamport: remote_lamport,
                        hint,
                    },
                    shared.downloads.clone(),
                )
                .await
                {
                    Ok(changes) => Ok(Some(changes)),
                    Err(err) => Err(err),
                }
            } else {
                shared.sync_activity.begin();
                shared.status.begin_sync(peer);
                match sync::reconcile_with_peer(
                    rpc_client,
                    Arc::clone(&shared.state),
                    shared.downloads.clone(),
                    peer,
                )
                .await
                {
                    Ok(changes) => Ok(Some(changes)),
                    Err(err) => Err(err),
                }
            };

        match sync_result {
            Ok(None) => {}
            Ok(Some(changes)) if changes.is_empty() => {
                tracing::info!("sync peer={} no changes applied", peer);
            }
            Ok(Some(changes)) => {
                tracing::info!("sync peer={} applied {} changes", peer, changes.len());
                let snapshot = {
                    let state = shared.state.read().await;
                    StateSnapshot::from_state(&state)
                };
                publish_sync_state(
                    &shared.publisher,
                    Arc::clone(&shared.group),
                    &snapshot,
                    &shared.local_origin,
                )
                .await;
            }
            Err(err) => {
                tracing::warn!("sync peer={} failed: {err}", peer);
            }
        }

        if shared.sync_activity.take_pending_rescan() {
            tracing::debug!("running deferred filesystem rescan after remote sync");
            apply_filesystem_paths(
                Arc::clone(&shared.state),
                &shared.publisher,
                Arc::clone(&shared.group),
                &shared.local_origin,
                Vec::new(),
            )
            .await;
        }
        shared.sync_activity.end();
        shared.status.end_sync(peer);

        if shared.sync_activity.take_pending_rescan() {
            tracing::debug!("running filesystem rescan queued at remote sync boundary");
            apply_filesystem_paths(
                Arc::clone(&shared.state),
                &shared.publisher,
                Arc::clone(&shared.group),
                &shared.local_origin,
                Vec::new(),
            )
            .await;
        }

        maybe_gc_tombstones(
            Arc::clone(&shared.state),
            Arc::clone(&shared.group),
            Arc::clone(&shared.root_reports),
            &shared.publisher,
            &shared.local_origin,
        )
        .await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::FolderState;
    use std::fs;

    #[test]
    fn filesystem_tree_hint_stays_under_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        let dir = tmp.path().join("a").join("b");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("c.txt"), "hello").unwrap();

        let changes = state.apply_paths(vec![tmp.path().join("a")]).unwrap();
        let snapshot = StateSnapshot::from_state(&state);
        let hint = build_tree_hint(&state, &changes, &snapshot, "node-a");

        assert!(hint.as_ref().is_some_and(|hint| !hint.nodes.is_empty()));
        assert!(
            filesystem_changed_len(&snapshot, "node-a", hint.as_ref())
                <= MAX_FILESYSTEM_CHANGED_BYTES
        );
    }
}
