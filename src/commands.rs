//! One-shot CLI subcommand implementations: `invite`, `peers`, `remove`,
//! `join`, `dump-state`, `scan`, and the per-folder daemon lock.

use crate::cli::{encode_ticket, parse_join_ticket};
use crate::discovery::{AddressBook, PeerInfo};
use crate::group::{GroupState, add_invite, generate_secret, now_ms};
use crate::identity::{Identity, NodeId};
use crate::rpc::RpcClient;
use crate::state::{Entry, EntryKind, hex, load_stored_entries};
use crate::tree::derive_tree;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub const KEY_FILE: &str = "private.key";
pub const PEERS_FILE: &str = "peers.json";
pub const INVITES_FILE: &str = "invites.json";
pub const PID_FILE: &str = "daemon.pid";

pub fn create_invite(state_dir: &Path, expire_secs: u64) -> io::Result<()> {
    let identity = Identity::load_or_create(&state_dir.join(KEY_FILE))?;
    let node_id = identity.node_id();
    let peers_path = state_dir.join(PEERS_FILE);
    let _group = GroupState::load_or_init(peers_path, node_id)?;
    let secret_bytes = generate_secret()?;
    let secret_hex = hex(secret_bytes);
    let expires_at = now_ms()? + expire_secs.saturating_mul(1000);
    add_invite(&state_dir.join(INVITES_FILE), &secret_hex, expires_at)?;
    println!("{}", encode_ticket(node_id, &secret_bytes));
    tracing::info!("invite expires in {}s", expire_secs);
    Ok(())
}

pub fn peers_cmd(state_dir: &Path) -> io::Result<()> {
    let identity = Identity::load_or_create(&state_dir.join(KEY_FILE))?;
    let node_id = identity.node_id();
    let peers_path = state_dir.join(PEERS_FILE);
    let group = GroupState::load_or_init(peers_path, node_id)?;
    for member in group.members() {
        let marker = if member.id == node_id.to_string() {
            " [self]"
        } else {
            ""
        };
        println!("{:?} {}{}", member.status, member.id, marker);
    }
    Ok(())
}

pub async fn scan_cmd(watch: bool) -> io::Result<()> {
    let address_book = crate::discovery::new_address_book();
    // Use a zero NodeId — we have no local identity, so nothing to filter out.
    let _mdns =
        crate::discovery::spawn_browser(NodeId::from_bytes([0; 32]), Arc::clone(&address_book))?;

    if watch {
        let frames = ["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"];
        let mut tick = 0usize;
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => break,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    render_scan(&address_book, Some(frames[tick % frames.len()])).await;
                    tick = tick.wrapping_add(1);
                }
            }
        }
    } else {
        tokio::time::sleep(Duration::from_secs(2)).await;
        render_scan(&address_book, None).await;
    }
    Ok(())
}

async fn render_scan(address_book: &AddressBook, spinner: Option<&str>) {
    if let Some(s) = spinner {
        print!("\x1b[2J\x1b[H");
        println!("lilsync nodes on LAN  {s}");
        println!();
    }
    let peers = address_book.read().await;
    if peers.is_empty() {
        println!("no peers found");
        let _ = io::stdout().flush();
        return;
    }
    println!("{:<64}  {:<16}  {:<6}  NAME", "ID", "IP", "PORT");
    let mut sorted: Vec<(&NodeId, &PeerInfo)> = peers.iter().collect();
    sorted.sort_by_key(|(id, _)| id.to_string());
    for (id, info) in sorted {
        println!(
            "{:<64}  {:<16}  {:<6}  {}",
            id.to_string(),
            info.addr.ip(),
            info.addr.port(),
            info.name.as_deref().unwrap_or("-"),
        );
    }
    let _ = io::stdout().flush();
}

pub fn status_cmd(folder: &Path) -> io::Result<()> {
    let state_dir = folder.join(".lil");

    println!("daemon    {}", daemon_running_status(&state_dir));

    let entries = load_stored_entries(folder)?;
    let live = entries
        .values()
        .filter(|e| e.kind != EntryKind::Tombstone)
        .count();
    let lamport = read_stored_lamport(&state_dir);
    let root = derive_tree(&entries).root_hash;
    println!(
        "root      {}  ({live} entries, lamport {lamport})",
        &hex(root)[..16]
    );
    println!();

    let identity = Identity::load_or_create(&state_dir.join(KEY_FILE))?;
    let node_id = identity.node_id();
    let group = GroupState::load_or_init(state_dir.join(PEERS_FILE), node_id)?;
    let members = group.members();
    let others: Vec<_> = members
        .iter()
        .filter(|m| m.id != node_id.to_string())
        .collect();

    println!("peers");
    for member in &members {
        let marker = if member.id == node_id.to_string() {
            "  [self]"
        } else {
            ""
        };
        println!(
            "  {:8}  {}{}",
            format!("{:?}", member.status),
            member.id.get(..16).unwrap_or(&member.id),
            marker
        );
    }
    if others.is_empty() {
        println!("  (none)");
    }

    Ok(())
}

fn daemon_running_status(state_dir: &Path) -> String {
    let pid_path = state_dir.join(PID_FILE);
    let Some(pid) = fs::read_to_string(&pid_path)
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
    else {
        return "stopped".to_string();
    };
    let alive = unsafe { libc::kill(pid, 0) } == 0;
    if alive {
        format!("running   pid {pid}")
    } else {
        "stopped   (stale pid file)".to_string()
    }
}

fn read_stored_lamport(state_dir: &Path) -> u64 {
    fs::read(state_dir.join("lamport"))
        .ok()
        .and_then(|b| b.try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0)
}

pub fn dump_state_cmd(folder: &Path, prefix: Option<&str>) -> io::Result<()> {
    let entries = load_stored_entries(folder)?;
    let prefix = prefix
        .map(|p| p.trim_matches('/'))
        .filter(|p| !p.is_empty());
    for entry in entries
        .values()
        .filter(|entry| entry_matches_prefix(entry, prefix))
    {
        println!("{}", entry_dump_json(entry)?);
    }
    Ok(())
}

fn entry_matches_prefix(entry: &Entry, prefix: Option<&str>) -> bool {
    let Some(prefix) = prefix else {
        return true;
    };
    entry.path == prefix
        || entry
            .path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn entry_dump_json(entry: &Entry) -> io::Result<String> {
    let hash = entry.content_hash.map(hex);
    serde_json::to_string(&serde_json::json!({
        "path": entry.path,
        "kind": entry_kind_name(entry.kind),
        "hash": hash,
        "target": entry.symlink_target,
        "size": entry.size,
        "mode": entry.mode,
        "lamport": entry.version.lamport,
        "origin": entry.version.origin,
    }))
    .map_err(io::Error::other)
}

fn entry_kind_name(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::File => "file",
        EntryKind::Dir => "dir",
        EntryKind::Tombstone => "tombstone",
        EntryKind::Symlink => "symlink",
    }
}

pub fn remove_peer_cmd(state_dir: &Path, target: &str) -> io::Result<()> {
    let identity = Identity::load_or_create(&state_dir.join(KEY_FILE))?;
    let node_id = identity.node_id();
    let peers_path = state_dir.join(PEERS_FILE);
    let mut group = GroupState::load_or_init(peers_path, node_id)?;
    match group.remove_peer(target)? {
        Some(id) => {
            println!("removed {id}");
            println!("restart the daemon to broadcast the removal to other peers");
        }
        None => {
            eprintln!("no peer found matching {target:?}");
            std::process::exit(1);
        }
    }
    Ok(())
}

pub async fn join_group(
    identity: Arc<Identity>,
    address_book: AddressBook,
    peers_path: &Path,
    ticket: &str,
) -> io::Result<()> {
    let ticket = parse_join_ticket(ticket)?;
    let rpc_client = RpcClient::new(Arc::clone(&identity), address_book);
    let members = rpc_client
        .join_group(ticket.issuer, ticket.secret, identity.node_id())
        .await?;
    GroupState::replace(peers_path, members)?;
    tracing::info!("joined group via {}", ticket.issuer);
    Ok(())
}

/// Fork into the background (double-fork), redirect stdin/stdout/stderr to
/// /dev/null, and return in the surviving grandchild. The two parent processes
/// exit immediately so the calling shell gets its prompt back.
#[cfg(unix)]
pub fn daemonize() -> io::Result<()> {
    unsafe {
        match libc::fork() {
            -1 => return Err(io::Error::last_os_error()),
            0 => {}
            _ => std::process::exit(0),
        }
        if libc::setsid() == -1 {
            return Err(io::Error::last_os_error());
        }
        match libc::fork() {
            -1 => return Err(io::Error::last_os_error()),
            0 => {}
            _ => std::process::exit(0),
        }
        let devnull = libc::open(c"/dev/null".as_ptr(), libc::O_RDWR);
        if devnull >= 0 {
            libc::dup2(devnull, libc::STDIN_FILENO);
            libc::dup2(devnull, libc::STDOUT_FILENO);
            libc::dup2(devnull, libc::STDERR_FILENO);
            libc::close(devnull);
        }
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn daemonize() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "--daemon is not supported on this platform",
    ))
}

#[cfg(unix)]
pub fn stop_cmd(state_dir: &Path) -> io::Result<()> {
    let pid_path = state_dir.join(PID_FILE);
    let contents = fs::read_to_string(&pid_path)
        .map_err(|_| io::Error::other("daemon is not running (no PID file found)"))?;
    let pid: libc::pid_t = contents
        .trim()
        .parse()
        .map_err(|_| io::Error::other("invalid PID file"))?;

    let rc = unsafe { libc::kill(pid, libc::SIGTERM) };
    if rc != 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            let _ = fs::remove_file(&pid_path);
            return Err(io::Error::other(
                "daemon is not running (stale PID file removed)",
            ));
        }
        return Err(err);
    }
    let _ = fs::remove_file(&pid_path);
    println!("stopped {pid}");
    Ok(())
}

#[cfg(not(unix))]
pub fn stop_cmd(_state_dir: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "lilsync stop is not supported on this platform",
    ))
}

#[cfg(unix)]
pub fn acquire_daemon_lock(state_dir: &Path) -> io::Result<fs::File> {
    use std::os::fd::AsRawFd;

    let lock_path = state_dir.join("daemon.lock");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return Err(io::Error::other(
                "another lilsync instance is already running on this folder",
            ));
        }
        return Err(err);
    }
    Ok(file)
}

#[cfg(not(unix))]
pub fn acquire_daemon_lock(state_dir: &Path) -> io::Result<fs::File> {
    let lock_path = state_dir.join("daemon.lock");
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(lock_path)
        .map_err(|err| {
            if err.kind() == io::ErrorKind::AlreadyExists {
                io::Error::other("another lilsync instance is already running on this folder")
            } else {
                err
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_key_is_reused_from_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let key_path = tmp.path().join(KEY_FILE);

        let first = Identity::load_or_create(&key_path).unwrap();
        let second = Identity::load_or_create(&key_path).unwrap();

        assert_eq!(first.node_id(), second.node_id());
        assert_eq!(fs::read(key_path).unwrap().len(), 32);
    }

    #[test]
    fn daemon_lock_rejects_second_holder() {
        let tmp = tempfile::tempdir().unwrap();

        let first = acquire_daemon_lock(tmp.path()).unwrap();
        let second = acquire_daemon_lock(tmp.path()).unwrap_err();

        assert_eq!(
            second.to_string(),
            "another lilsync instance is already running on this folder"
        );
        drop(first);
        let _third = acquire_daemon_lock(tmp.path()).unwrap();
    }
}
