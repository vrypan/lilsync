//! Peer endpoint address book: maps `NodeId` to a list of `host:port`
//! endpoints, persisted in `.lil/endpoints.json`. Endpoints are learned from
//! join tickets and from `Endpoints` gossip messages, replacing mDNS
//! discovery. Also owns the persisted RPC listen port (`.lil/port`) and
//! local interface enumeration for advertising this node's own endpoints.

use crate::identity::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

pub const ENDPOINTS_FILE: &str = "endpoints.json";
pub const PORT_FILE: &str = "port";

/// Cap per-peer endpoint lists so a misbehaving member cannot grow the
/// address book without bound.
pub const MAX_ENDPOINTS_PER_PEER: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Candidate `host:port` endpoints, most recently confirmed first.
    pub endpoints: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

pub type AddressBook = Arc<RwLock<HashMap<NodeId, PeerInfo>>>;

pub fn new_address_book() -> AddressBook {
    Arc::new(RwLock::new(HashMap::new()))
}

pub fn load_address_book(state_dir: &Path) -> HashMap<NodeId, PeerInfo> {
    let path = state_dir.join(ENDPOINTS_FILE);
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(err) => {
            if err.kind() != io::ErrorKind::NotFound {
                tracing::warn!("could not read {}: {err}", path.display());
            }
            return HashMap::new();
        }
    };
    let parsed: HashMap<String, PeerInfo> = match serde_json::from_str(&contents) {
        Ok(parsed) => parsed,
        Err(err) => {
            tracing::warn!("invalid endpoints file {}: {err}", path.display());
            return HashMap::new();
        }
    };
    parsed
        .into_iter()
        .filter_map(|(id, info)| Some((id.parse::<NodeId>().ok()?, info)))
        .collect()
}

pub fn save_address_book(state_dir: &Path, book: &HashMap<NodeId, PeerInfo>) -> io::Result<()> {
    let by_id: HashMap<String, &PeerInfo> = book
        .iter()
        .map(|(id, info)| (id.to_string(), info))
        .collect();
    let json = serde_json::to_string_pretty(&by_id).map_err(io::Error::other)?;
    let path = state_dir.join(ENDPOINTS_FILE);
    let tmp = state_dir.join(format!("{ENDPOINTS_FILE}.tmp"));
    fs::write(&tmp, json)?;
    fs::rename(&tmp, &path)
}

pub fn load_port(state_dir: &Path) -> Option<u16> {
    fs::read_to_string(state_dir.join(PORT_FILE))
        .ok()?
        .trim()
        .parse()
        .ok()
}

pub fn save_port(state_dir: &Path, port: u16) -> io::Result<()> {
    fs::write(state_dir.join(PORT_FILE), port.to_string())
}

/// `host:port` endpoints for every usable local interface address, IPv4
/// first. These are what this node advertises to peers.
pub fn local_endpoints(port: u16) -> Vec<String> {
    let mut addrs: Vec<IpAddr> = match if_addrs::get_if_addrs() {
        Ok(ifaces) => ifaces
            .into_iter()
            .map(|iface| iface.addr.ip())
            .filter(|ip| !ip.is_loopback() && !ip.is_unspecified())
            .filter(|ip| match ip {
                IpAddr::V4(_) => true,
                // Link-local IPv6 needs a scope id to be routable; skip it.
                IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) != 0xfe80,
            })
            .collect(),
        Err(err) => {
            tracing::warn!("could not enumerate local interfaces: {err}");
            Vec::new()
        }
    };
    addrs.sort_by_key(|ip| ip.is_ipv6());
    addrs.dedup();
    addrs
        .into_iter()
        .take(MAX_ENDPOINTS_PER_PEER)
        .map(|ip| format_endpoint(ip, port))
        .collect()
}

pub fn format_endpoint(ip: IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(v4) => format!("{v4}:{port}"),
        IpAddr::V6(v6) => format!("[{v6}]:{port}"),
    }
}

/// Merge advertised endpoints for `peer` into the address book, putting
/// `observed` (the address the peer was actually seen from) first. Returns
/// true if the entry changed.
pub async fn merge_peer_endpoints(
    book: &AddressBook,
    peer: NodeId,
    observed: Option<String>,
    advertised: &[String],
    name: Option<String>,
) -> bool {
    let mut merged: Vec<String> = Vec::new();
    for endpoint in observed.iter().chain(advertised.iter()) {
        if !merged.contains(endpoint) && merged.len() < MAX_ENDPOINTS_PER_PEER {
            merged.push(endpoint.clone());
        }
    }
    if merged.is_empty() {
        return false;
    }
    let mut book = book.write().await;
    match book.get_mut(&peer) {
        Some(info) if info.endpoints == merged && info.name == name => false,
        Some(info) => {
            info.endpoints = merged;
            info.name = name;
            true
        }
        None => {
            book.insert(
                peer,
                PeerInfo {
                    endpoints: merged,
                    name,
                },
            );
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_book_roundtrips_through_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let id = NodeId::from_bytes([7; 32]);
        let mut book = HashMap::new();
        book.insert(
            id,
            PeerInfo {
                endpoints: vec!["100.64.1.2:7420".to_string(), "192.168.1.9:7420".to_string()],
                name: Some("laptop".to_string()),
            },
        );
        save_address_book(tmp.path(), &book).unwrap();
        let loaded = load_address_book(tmp.path());
        assert_eq!(loaded.len(), 1);
        let info = &loaded[&id];
        assert_eq!(info.endpoints, book[&id].endpoints);
        assert_eq!(info.name.as_deref(), Some("laptop"));
    }

    #[test]
    fn port_roundtrips_through_disk() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(load_port(tmp.path()), None);
        save_port(tmp.path(), 7420).unwrap();
        assert_eq!(load_port(tmp.path()), Some(7420));
    }

    #[tokio::test]
    async fn merge_puts_observed_endpoint_first_and_dedupes() {
        let book = new_address_book();
        let peer = NodeId::from_bytes([1; 32]);
        let advertised = vec!["10.0.0.5:7420".to_string(), "100.64.1.2:7420".to_string()];
        let changed = merge_peer_endpoints(
            &book,
            peer,
            Some("100.64.1.2:7420".to_string()),
            &advertised,
            None,
        )
        .await;
        assert!(changed);
        let info = book.read().await[&peer].clone();
        assert_eq!(info.endpoints[0], "100.64.1.2:7420");
        assert_eq!(info.endpoints.len(), 2);

        // Re-merging identical data reports no change.
        let changed = merge_peer_endpoints(
            &book,
            peer,
            Some("100.64.1.2:7420".to_string()),
            &advertised,
            None,
        )
        .await;
        assert!(!changed);
    }

    #[test]
    fn format_endpoint_brackets_ipv6() {
        assert_eq!(
            format_endpoint("2001:db8::1".parse().unwrap(), 7420),
            "[2001:db8::1]:7420"
        );
        assert_eq!(format_endpoint("10.0.0.1".parse().unwrap(), 80), "10.0.0.1:80");
    }
}
