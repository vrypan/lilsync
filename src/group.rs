//! Group membership ledger: tracks active/removed members, generates and
//! validates one-time invite tokens, and merges peer lists received over RPC.

use crate::identity::NodeId;
use crate::state::hex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MemberStatus {
    Active,
    Removed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberEntry {
    pub id: String,
    pub status: MemberStatus,
    pub lamport: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeersFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    topic_id: Option<String>,
    #[serde(default)]
    members: Vec<MemberEntry>,
}

#[derive(Debug)]
pub struct GroupState {
    path: PathBuf,
    local_id: String,
    members: BTreeMap<String, MemberEntry>,
}

#[derive(Debug)]
pub struct MemberMerge {
    pub changed: bool,
    pub removed_self: bool,
    pub active_peers: Vec<NodeId>,
}

impl GroupState {
    pub fn load_or_init(path: PathBuf, local_id: NodeId) -> io::Result<Self> {
        let local_id = local_id.to_string();
        let loaded = match fs::read_to_string(&path) {
            Ok(contents) => Some(serde_json::from_str::<PeersFile>(&contents).map_err(|err| {
                io::Error::other(format!("invalid peers file {}: {err}", path.display()))
            })?),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(err),
        };

        let mut members: BTreeMap<String, MemberEntry> = loaded
            .map(|file| {
                file.members
                    .into_iter()
                    .map(|entry| (entry.id.clone(), entry))
                    .collect()
            })
            .unwrap_or_default();

        if !members.contains_key(&local_id) {
            let lamport = next_lamport(&members);
            members.insert(
                local_id.clone(),
                MemberEntry {
                    id: local_id.clone(),
                    status: MemberStatus::Active,
                    lamport,
                },
            );
        }

        let state = Self {
            path,
            local_id,
            members,
        };
        state.persist()?;
        Ok(state)
    }

    pub fn replace(path: &Path, members: Vec<MemberEntry>) -> io::Result<()> {
        save_peers_file(path, members)
    }

    pub fn members(&self) -> Vec<MemberEntry> {
        self.members.values().cloned().collect()
    }

    pub fn active_peers(&self) -> Vec<NodeId> {
        active_peers_from_members(&self.local_id, self.members.values())
    }

    pub fn active_peer_ids(&self) -> Vec<String> {
        self.members
            .values()
            .filter(|entry| entry.id != self.local_id)
            .filter(|entry| entry.status == MemberStatus::Active)
            .map(|entry| entry.id.clone())
            .collect()
    }

    pub fn is_active_member(&self, peer: &NodeId) -> bool {
        self.members
            .get(&peer.to_string())
            .is_some_and(|entry| entry.status == MemberStatus::Active)
    }

    pub fn is_known_member(&self, peer: &NodeId) -> bool {
        self.members.contains_key(&peer.to_string())
    }

    pub fn add_active_peer(&mut self, peer: NodeId) -> io::Result<bool> {
        let id = peer.to_string();
        let lamport = next_lamport(&self.members);
        let changed = match self.members.get_mut(&id) {
            Some(entry) if entry.status == MemberStatus::Active => false,
            Some(entry) => {
                entry.status = MemberStatus::Active;
                entry.lamport = lamport;
                true
            }
            None => {
                self.members.insert(
                    id.clone(),
                    MemberEntry {
                        id,
                        status: MemberStatus::Active,
                        lamport,
                    },
                );
                true
            }
        };
        if changed {
            self.persist()?;
        }
        Ok(changed)
    }

    pub fn remove_peer(&mut self, target: &str) -> io::Result<Option<String>> {
        let id = self
            .members
            .values()
            .find(|e| e.id == target)
            .map(|e| e.id.clone());

        let Some(id) = id else {
            return Ok(None);
        };
        if id == self.local_id {
            return Err(io::Error::other("cannot remove self from the group"));
        }

        let lamport = next_lamport(&self.members);
        let entry = self.members.get_mut(&id).unwrap();
        entry.status = MemberStatus::Removed;
        entry.lamport = lamport;
        self.persist()?;
        Ok(Some(id))
    }

    pub fn merge_members(&mut self, incoming: Vec<MemberEntry>) -> io::Result<MemberMerge> {
        let mut changed = false;
        let mut removed_self = false;

        for entry in incoming {
            let apply = self
                .members
                .get(&entry.id)
                .is_none_or(|local| entry.lamport > local.lamport);
            if !apply {
                continue;
            }
            if entry.id == self.local_id && entry.status == MemberStatus::Removed {
                removed_self = true;
            }
            self.members.insert(entry.id.clone(), entry);
            changed = true;
        }

        if changed {
            self.persist()?;
        }

        Ok(MemberMerge {
            changed,
            removed_self,
            active_peers: self.active_peers(),
        })
    }

    fn persist(&self) -> io::Result<()> {
        save_peers_file(&self.path, self.members())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PendingInvite {
    secret_hash: String,
    expires_at_ms: u64,
}

pub fn add_invite(path: &Path, secret: &str, expires_at_ms: u64) -> io::Result<()> {
    let mut invites = load_invites(path)?;
    let now = now_ms()?;
    invites.retain(|invite| invite.expires_at_ms > now);
    invites.push(PendingInvite {
        secret_hash: hash_secret(secret),
        expires_at_ms,
    });
    save_invites(path, &invites)
}

pub fn consume_invite(path: &Path, secret: &str) -> io::Result<bool> {
    let mut invites = load_invites(path)?;
    let original_len = invites.len();
    let now = now_ms()?;
    let secret_hash = hash_secret(secret);
    let mut found = false;

    invites.retain(|invite| {
        if invite.expires_at_ms <= now {
            return false;
        }
        if invite.secret_hash == secret_hash {
            found = true;
            return false;
        }
        true
    });

    // Only rewrite the file if something actually changed: a consumed token or
    // pruned expired tokens. A failed attempt against a valid set of invites
    // must not trigger a disk write, so spamming Join cannot amplify into I/O.
    if invites.len() != original_len {
        save_invites(path, &invites)?;
    }
    Ok(found)
}

pub fn generate_secret() -> io::Result<[u8; 32]> {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).map_err(io::Error::other)?;
    Ok(bytes)
}

pub fn now_ms() -> io::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .map_err(io::Error::other)
}

fn active_peers_from_members<'a>(
    local_id: &str,
    members: impl IntoIterator<Item = &'a MemberEntry>,
) -> Vec<NodeId> {
    members
        .into_iter()
        .filter(|entry| entry.id != local_id)
        .filter(|entry| entry.status == MemberStatus::Active)
        .filter_map(|entry| entry.id.parse().ok())
        .collect()
}

fn save_peers_file(path: &Path, mut members: Vec<MemberEntry>) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    members.sort_by(|left, right| left.id.cmp(&right.id));
    let file = PeersFile {
        topic_id: None,
        members,
    };
    let json = serde_json::to_string_pretty(&file).map_err(io::Error::other)?;
    fs::write(path, json)
}

fn next_lamport(members: &BTreeMap<String, MemberEntry>) -> u64 {
    members
        .values()
        .map(|entry| entry.lamport)
        .max()
        .unwrap_or(0)
        + 1
}

fn load_invites(path: &Path) -> io::Result<Vec<PendingInvite>> {
    match fs::read_to_string(path) {
        Ok(contents) => serde_json::from_str(&contents).map_err(io::Error::other),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}

fn save_invites(path: &Path, invites: &[PendingInvite]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(invites).map_err(io::Error::other)?;
    fs::write(path, json)
}

fn hash_secret(secret: &str) -> String {
    hex(*blake3::hash(secret.as_bytes()).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 32])
    }

    #[test]
    fn invite_is_one_time() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("invites.json");

        add_invite(&path, "secret", now_ms().unwrap() + 10_000).unwrap();

        assert!(consume_invite(&path, "secret").unwrap());
        assert!(!consume_invite(&path, "secret").unwrap());
    }

    #[test]
    fn member_merge_keeps_newest_lamport() {
        let tmp = tempfile::tempdir().unwrap();
        let local = node(1);
        let peer = node(2);
        let mut state = GroupState::load_or_init(tmp.path().join("peers.json"), local).unwrap();

        let peer_id = peer.to_string();
        let update = state
            .merge_members(vec![MemberEntry {
                id: peer_id.clone(),
                status: MemberStatus::Active,
                lamport: 2,
            }])
            .unwrap();
        assert!(update.changed);
        assert_eq!(update.active_peers, vec![peer]);

        let update = state
            .merge_members(vec![MemberEntry {
                id: peer_id,
                status: MemberStatus::Removed,
                lamport: 1,
            }])
            .unwrap();
        assert!(!update.changed);
        assert_eq!(update.active_peers, vec![peer]);
    }

    #[test]
    fn remove_peer_by_id() {
        let tmp = tempfile::tempdir().unwrap();
        let local = node(1);
        let peer = node(2);
        let path = tmp.path().join("peers.json");
        let mut state = GroupState::load_or_init(path, local).unwrap();

        state.add_active_peer(peer).unwrap();
        assert!(state.is_active_member(&peer));

        let removed = state.remove_peer(&peer.to_string()).unwrap();
        assert_eq!(removed.as_deref(), Some(peer.to_string().as_str()));
        assert!(!state.is_active_member(&peer));

        state.add_active_peer(peer).unwrap();
        let removed = state.remove_peer(&peer.to_string()).unwrap();
        assert!(removed.is_some());
    }

    #[test]
    fn remove_self_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let local = node(1);
        let path = tmp.path().join("peers.json");
        let mut state = GroupState::load_or_init(path, local).unwrap();
        assert!(state.remove_peer(&local.to_string()).is_err());
    }
}
