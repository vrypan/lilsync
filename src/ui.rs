//! Terminal output: the live status view, change/peer/state log helpers, and
//! the `StateSnapshot` and `StatusState` types shared with the daemon.

use crate::discovery::AddressBook;
use crate::group::{GroupState, MemberStatus};
use crate::identity::NodeId;
use crate::message::GossipMessage;
use crate::state::{Change, FolderState, hex};
use std::io::{self, Write};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::RwLock;

pub struct StatusState {
    current_sync: StdMutex<Option<NodeId>>,
}

impl Default for StatusState {
    fn default() -> Self {
        Self {
            current_sync: StdMutex::new(None),
        }
    }
}

impl StatusState {
    pub fn begin_sync(&self, peer: NodeId) {
        *self.current_sync.lock().expect("status state poisoned") = Some(peer);
    }

    pub fn end_sync(&self, peer: NodeId) {
        let mut current = self.current_sync.lock().expect("status state poisoned");
        if current.is_some_and(|active| active == peer) {
            *current = None;
        }
    }

    pub fn current_sync(&self) -> Option<NodeId> {
        *self.current_sync.lock().expect("status state poisoned")
    }
}

#[derive(Clone)]
pub struct StateSnapshot {
    pub state_root: [u8; 32],
    pub live_root: [u8; 32],
    pub lamport: u64,
    pub state_nodes: usize,
    pub live_nodes: usize,
    pub gc_watermark: crate::state::GcWatermark,
}

impl StateSnapshot {
    pub fn from_state(state: &FolderState) -> Self {
        Self {
            state_root: state.root_hash(),
            live_root: state.live_root_hash(),
            lamport: state.lamport(),
            state_nodes: state.tree().nodes.len(),
            live_nodes: state.live_tree().nodes.len(),
            gc_watermark: state.gc_watermark().clone(),
        }
    }
}

pub async fn status_view_loop(
    state: Arc<RwLock<FolderState>>,
    group: Arc<RwLock<GroupState>>,
    root_reports: Arc<RwLock<crate::daemon::RootReports>>,
    address_book: AddressBook,
    status: Arc<StatusState>,
    local_origin: String,
) {
    let frames = ["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"];
    let mut tick = 0usize;
    loop {
        render_status_view(
            &state,
            &group,
            &root_reports,
            &address_book,
            &status,
            &local_origin,
            frames[tick % frames.len()],
        )
        .await;
        tick = tick.wrapping_add(1);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn render_status_view(
    state: &Arc<RwLock<FolderState>>,
    group: &Arc<RwLock<GroupState>>,
    root_reports: &Arc<RwLock<crate::daemon::RootReports>>,
    address_book: &AddressBook,
    status: &StatusState,
    local_origin: &str,
    spinner: &str,
) {
    let local = {
        let state = state.read().await;
        StateSnapshot::from_state(&state)
    };
    let members = group.read().await.members();
    let reports = root_reports.read().await;
    let addresses = address_book.read().await;
    let syncing_peer = status.current_sync();

    print!("\x1b[2J\x1b[H");
    println!("lilsync {}", short_id(local_origin));
    println!(
        "root {}  entries {}",
        hex(local.state_root),
        local.state_nodes
    );
    println!();

    let mut shown = 0usize;
    for member in members
        .iter()
        .filter(|member| member.id != local_origin)
        .filter(|member| member.status == MemberStatus::Active)
    {
        shown += 1;
        let peer = member.id.parse::<NodeId>().ok();
        let online = peer.is_some_and(|peer| addresses.contains_key(&peer));
        let is_syncing = peer.is_some_and(|peer| syncing_peer == Some(peer));
        let synced = reports.is_synced(&member.id, &local);
        let label = peer
            .and_then(|p| addresses.get(&p))
            .and_then(|info| info.name.as_deref())
            .unwrap_or(&member.id);
        let status_text = if !online {
            "\x1b[31moffline\x1b[0m  ".to_string()
        } else if is_syncing || !synced {
            format!("\x1b[33msyncing\x1b[0m {spinner}")
        } else {
            "\x1b[32msynced\x1b[0m   ".to_string()
        };
        println!("{} {}", status_text, truncate_label(label));
    }
    if shown == 0 {
        println!("no peers");
    }
    let _ = io::stdout().flush();
}

pub fn print_start(
    state: &FolderState,
    local_id: NodeId,
    rpc_port: u16,
    bootstrap: &[NodeId],
    poll: bool,
    interval_ms: u64,
) {
    tracing::info!("node {}", local_id);
    tracing::info!("rpc port {}", rpc_port);
    tracing::info!("invite command: lilsync invite {}", state.root().display());
    tracing::info!("known peers {}", bootstrap.len());
    tracing::info!("watching {}", state.root().display());
    if poll {
        tracing::info!("mode poll interval={}ms", interval_ms.max(50));
    } else {
        tracing::info!("mode fs-events debounce={}ms", interval_ms.max(20));
    }
    tracing::info!("state root {}", hex(state.root_hash()));
    tracing::info!("live root {}", hex(state.live_root_hash()));
    tracing::info!(
        "nodes state={} live={}",
        state.tree().nodes.len(),
        state.live_tree().nodes.len()
    );
}

pub fn print_changes(
    before_state: [u8; 32],
    before_live: [u8; 32],
    state: &FolderState,
    changes: &[Change],
) {
    for change in changes {
        tracing::info!(
            "{} {} v{}:{}",
            change.verb(),
            change.path,
            change.new.version.lamport,
            change.new.version.origin
        );
    }

    tracing::info!(
        "state root {} -> {} nodes={}",
        hex(before_state),
        hex(state.root_hash()),
        state.tree().nodes.len()
    );
    tracing::info!(
        "live root {} -> {} nodes={}",
        hex(before_live),
        hex(state.live_root_hash()),
        state.live_tree().nodes.len()
    );
}

pub fn print_remote_message(message: &GossipMessage, state: &StateSnapshot) {
    match message {
        GossipMessage::SyncState {
            origin,
            state_root,
            live_root,
            lamport,
            state_nodes,
            live_nodes,
            gc_watermark,
        } => {
            tracing::info!(
                "peer state origin={} lamport={} state_root={} live_root={} state_nodes={} live_nodes={} gc_origins={} state_match={} live_match={}",
                origin,
                lamport,
                hex(*state_root),
                hex(*live_root),
                state_nodes,
                live_nodes,
                gc_watermark.len(),
                *state_root == state.state_root,
                *live_root == state.live_root
            );
        }
        GossipMessage::FilesystemChanged {
            origin,
            state_root,
            live_root,
            lamport,
            hint,
        } => {
            let hint_nodes = hint.as_ref().map(|hint| hint.nodes.len()).unwrap_or(0);
            let hint_truncated = hint.as_ref().map(|hint| hint.truncated).unwrap_or(false);
            tracing::info!(
                "peer filesystem origin={} lamport={} hint_nodes={} hint_truncated={} state_root={} live_root={} state_match={} live_match={}",
                origin,
                lamport,
                hint_nodes,
                hint_truncated,
                hex(*state_root),
                hex(*live_root),
                *state_root == state.state_root,
                *live_root == state.live_root
            );
        }
        GossipMessage::Peers { origin, members } => {
            tracing::info!("peer list origin={} members={}", origin, members.len());
        }
    }
}

pub fn summarize_message(message: &GossipMessage) -> String {
    match message {
        GossipMessage::SyncState {
            origin,
            state_root,
            lamport,
            ..
        } => format!(
            "sync-state origin={origin} lamport={lamport} state_root={}",
            hex(*state_root)
        ),
        GossipMessage::FilesystemChanged {
            origin,
            lamport,
            hint,
            ..
        } => {
            let hint_nodes = hint.as_ref().map(|hint| hint.nodes.len()).unwrap_or(0);
            format!("filesystem-changed origin={origin} lamport={lamport} hint_nodes={hint_nodes}")
        }
        GossipMessage::Peers { origin, members } => {
            format!("peers origin={origin} members={}", members.len())
        }
    }
}

fn short_id(id: &str) -> String {
    id.chars().take(12).collect()
}

fn truncate_label(label: &str) -> String {
    const MAX_LABEL: usize = 32;
    let mut chars = label.chars();
    let mut out: String = chars.by_ref().take(MAX_LABEL).collect();
    if chars.next().is_some() {
        out.push_str("...");
    }
    out
}
