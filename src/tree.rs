//! Merkle tree over the entry index. Provides `TreeNode`/`TreeSnapshot` types,
//! full derivation from scratch, and incremental update after a set of path
//! changes. Hashing uses BLAKE3 with a deterministic encoding of entry fields.

use crate::entries::{Entry, EntryKind};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TreeNode {
    pub prefix: String,
    pub entries: BTreeMap<String, [u8; 32]>,
    pub children: BTreeMap<String, [u8; 32]>,
    pub hash: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct TreeSnapshot {
    pub root_hash: [u8; 32],
    pub nodes: BTreeMap<String, TreeNode>,
}

impl TreeSnapshot {
    pub(crate) fn empty() -> Self {
        let mut nodes = BTreeMap::new();
        let root = hash_node("/", &BTreeMap::new(), &BTreeMap::new());
        nodes.insert(
            String::new(),
            TreeNode {
                prefix: "/".to_string(),
                entries: BTreeMap::new(),
                children: BTreeMap::new(),
                hash: root,
            },
        );
        Self {
            root_hash: root,
            nodes,
        }
    }
}

pub(crate) fn derive_tree(entries: &BTreeMap<String, Entry>) -> TreeSnapshot {
    derive_tree_with(entries, |_| true)
}

pub(crate) fn derive_live_tree(entries: &BTreeMap<String, Entry>) -> TreeSnapshot {
    derive_tree_with(entries, |entry| entry.kind != EntryKind::Tombstone)
}

pub(crate) fn update_tree_snapshot(
    snapshot: &mut TreeSnapshot,
    entries: &BTreeMap<String, Entry>,
    changed_paths: &[String],
    include: impl Fn(&Entry) -> bool,
) {
    let mut dirty: BTreeSet<String> = BTreeSet::new();
    for path in changed_paths {
        dirty.insert(path.clone());
        dirty.extend(ancestors(path));
    }

    for path in changed_paths {
        if !snapshot.nodes.contains_key(path) {
            continue;
        }
        let still_included = entries.get(path).is_some_and(&include);
        if still_included {
            continue;
        }
        let prefix = format!("{path}/");
        let descendants: Vec<String> = snapshot
            .nodes
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        for desc in descendants {
            dirty.extend(ancestors(&desc));
            dirty.insert(desc);
        }
    }

    let mut by_parent: BTreeMap<String, Vec<&Entry>> = BTreeMap::new();
    for entry in entries.values() {
        if include(entry) {
            by_parent
                .entry(parent_path(&entry.path))
                .or_default()
                .push(entry);
        }
    }

    // Key-only child index over existing nodes plus every dirty path (dirty
    // paths may be inserted as new nodes during the pass below). Hashes are
    // read from the live snapshot so children updated or removed earlier in
    // the bottom-up walk are observed correctly.
    let mut child_keys: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for key in snapshot.nodes.keys().chain(dirty.iter()) {
        if key.is_empty() {
            continue;
        }
        child_keys
            .entry(parent_path(key))
            .or_default()
            .insert(key.clone());
    }

    for node_path in dirty.iter().rev() {
        let direct_entries: BTreeMap<String, [u8; 32]> = by_parent
            .get(node_path)
            .map(|es| {
                es.iter()
                    .map(|e| (basename(&e.path), hash_entry(e)))
                    .collect()
            })
            .unwrap_or_default();

        let direct_children: BTreeMap<String, [u8; 32]> = child_keys
            .get(node_path)
            .map(|keys| {
                keys.iter()
                    .filter_map(|k| snapshot.nodes.get(k).map(|v| (basename(k), v.hash)))
                    .collect()
            })
            .unwrap_or_default();

        if node_path.is_empty() || !direct_entries.is_empty() || !direct_children.is_empty() {
            let prefix = display_prefix(node_path);
            let hash = hash_node(&prefix, &direct_entries, &direct_children);
            snapshot.nodes.insert(
                node_path.clone(),
                TreeNode {
                    prefix,
                    entries: direct_entries,
                    children: direct_children,
                    hash,
                },
            );
        } else {
            snapshot.nodes.remove(node_path);
        }
    }

    snapshot.root_hash = snapshot.nodes.get("").map(|n| n.hash).unwrap_or([0; 32]);
}

fn derive_tree_with(
    entries: &BTreeMap<String, Entry>,
    include: impl Fn(&Entry) -> bool,
) -> TreeSnapshot {
    let mut node_paths = BTreeSet::new();
    node_paths.insert(String::new());

    for entry in entries.values() {
        if !include(entry) {
            continue;
        }
        for ancestor in ancestors(&entry.path) {
            node_paths.insert(ancestor);
        }
        if entry.kind == EntryKind::Dir {
            node_paths.insert(entry.path.clone());
        }
    }

    let mut partials = BTreeMap::new();
    for path in &node_paths {
        partials.insert(
            path.clone(),
            TreeNode {
                prefix: display_prefix(path),
                entries: BTreeMap::new(),
                children: BTreeMap::new(),
                hash: [0; 32],
            },
        );
    }

    for entry in entries.values() {
        if !include(entry) {
            continue;
        }
        let parent = parent_path(&entry.path);
        let name = basename(&entry.path);
        if let Some(node) = partials.get_mut(&parent) {
            node.entries.insert(name, hash_entry(entry));
        }
    }

    let mut children_of: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for candidate in &node_paths {
        if candidate.is_empty() {
            continue;
        }
        children_of
            .entry(parent_path(candidate))
            .or_default()
            .push(candidate.clone());
    }

    let mut hashes = BTreeMap::new();
    for path in node_paths.iter().rev() {
        let child_paths: &[String] = children_of.get(path).map(|v| v.as_slice()).unwrap_or(&[]);

        let mut children = BTreeMap::new();
        for child in child_paths {
            if let Some(hash) = hashes.get(child) {
                children.insert(basename(child), *hash);
            }
        }

        let node = partials
            .get_mut(path)
            .expect("node path came from the same set");
        node.children = children;
        node.hash = hash_node(&node.prefix, &node.entries, &node.children);
        hashes.insert(path.clone(), node.hash);
    }

    TreeSnapshot {
        root_hash: hashes.get("").copied().unwrap_or([0; 32]),
        nodes: partials,
    }
}

fn ancestors(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = parent_path(path);
    loop {
        out.push(current.clone());
        if current.is_empty() {
            break;
        }
        current = parent_path(&current);
    }
    out
}

pub(crate) fn normalize_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

fn parent_path(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

fn basename(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(_, name)| name.to_string())
        .unwrap_or_else(|| path.to_string())
}

fn display_prefix(path: &str) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{path}/")
    }
}

fn hash_entry(entry: &Entry) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    encode_str(&mut hasher, "lil-entry-v1");
    encode_str(&mut hasher, &entry.path);
    encode_str(&mut hasher, entry.kind.as_str());
    encode_opt_hash(&mut hasher, entry.content_hash);
    encode_opt_str(&mut hasher, entry.symlink_target.as_deref());
    hasher.update(&entry.size.to_be_bytes());
    encode_opt_u32(&mut hasher, entry.mode);
    hasher.update(&entry.version.lamport.to_be_bytes());
    encode_str(&mut hasher, &entry.version.origin);
    *hasher.finalize().as_bytes()
}

pub(crate) fn hash_node(
    prefix: &str,
    entries: &BTreeMap<String, [u8; 32]>,
    children: &BTreeMap<String, [u8; 32]>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"lil-node-v1");
    encode_str(&mut hasher, prefix);
    for (name, hash) in entries {
        encode_str(&mut hasher, "entry");
        encode_str(&mut hasher, name);
        hasher.update(hash);
    }
    for (name, hash) in children {
        encode_str(&mut hasher, "child");
        encode_str(&mut hasher, name);
        hasher.update(hash);
    }
    *hasher.finalize().as_bytes()
}

fn encode_str(hasher: &mut blake3::Hasher, value: &str) {
    let bytes = value.as_bytes();
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn encode_opt_hash(hasher: &mut blake3::Hasher, value: Option<[u8; 32]>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&value);
        }
        None => {
            hasher.update(&[0]);
        }
    };
}

fn encode_opt_str(hasher: &mut blake3::Hasher, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            encode_str(hasher, value);
        }
        None => {
            hasher.update(&[0]);
        }
    };
}

fn encode_opt_u32(hasher: &mut blake3::Hasher, value: Option<u32>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&value.to_be_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    };
}

pub fn entry_hash(entry: &Entry) -> [u8; 32] {
    hash_entry(entry)
}

pub fn tree_node_hash(node: &TreeNode) -> [u8; 32] {
    hash_node(&node.prefix, &node.entries, &node.children)
}
