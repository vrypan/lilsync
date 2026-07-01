//! `FolderState`: the authoritative on-disk and in-memory sync state for one
//! watched folder — entry index, Merkle trees, Lamport clock, and GC
//! watermark. Also re-exports all public entry and tree types so that callers
//! only need to import from this module.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use crate::entries::{
    placeholder_version, same_observed_state, tombstone_entry, validate_symlink_target,
};
use crate::ignore::{IGNORE_FILE_NAME, STATE_DIR, load_ignore_patterns, should_ignore};
use crate::scan::{
    mode, normalize_event_path, observe_file_when_stable, relative_path, scan_dir, scan_folder,
};
use crate::tree::{
    TreeSnapshot, derive_live_tree, derive_tree, normalize_prefix, update_tree_snapshot,
};

pub use crate::entries::{Change, Entry, EntryKind, GcWatermark, Version, hex};
pub use crate::tree::{TreeNode, entry_hash, tree_node_hash};

static NEXT_RECV_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, serde::Deserialize)]
struct LegacyEntry {
    path: String,
    kind: EntryKind,
    content_hash: Option<[u8; 32]>,
    size: u64,
    mode: Option<u32>,
    version: Version,
}

impl LegacyEntry {
    fn into_entry(self) -> Entry {
        Entry {
            path: self.path,
            kind: self.kind,
            content_hash: self.content_hash,
            symlink_target: None,
            size: self.size,
            mode: self.mode,
            version: self.version,
        }
    }
}

use std::collections::BTreeMap;

#[derive(Debug)]
pub struct FolderState {
    root: PathBuf,
    origin: String,
    lamport: u64,
    gc_watermark: GcWatermark,
    entries: BTreeMap<String, Entry>,
    tree: TreeSnapshot,
    live_tree: TreeSnapshot,
}

impl FolderState {
    pub fn new(root: PathBuf, origin: String) -> io::Result<Self> {
        let root = fs::canonicalize(root)?;
        let state_dir = root.join(STATE_DIR);
        fs::create_dir_all(&state_dir)?;
        cleanup_recv_files(&state_dir);
        let saved_lamport = load_saved_lamport(&state_dir);
        let gc_watermark = load_gc_watermark(&state_dir);
        let saved_entries = load_saved_entries(&state_dir)?;
        // The clock is persisted separately from the entries; if its file
        // lags or is lost, trusting it would issue versions below ones
        // peers already hold, silently blocking local changes. Never start
        // below the highest version present in the loaded index.
        let max_entry_lamport = saved_entries
            .values()
            .map(|entry| entry.version.lamport)
            .max()
            .unwrap_or(0);
        let mut state = Self {
            root,
            origin,
            lamport: saved_lamport.max(max_entry_lamport),
            gc_watermark,
            entries: saved_entries,
            tree: TreeSnapshot::empty(),
            live_tree: TreeSnapshot::empty(),
        };
        if state.prune_tombstones_covered_by_watermark() > 0 {
            state.save_entries();
        }
        tracing::info!("scan started");
        let scan_start = std::time::Instant::now();
        let changes = state.rescan()?;
        tracing::info!(
            "scan done elapsed_ms={} changes={}",
            scan_start.elapsed().as_millis(),
            changes.len()
        );
        if !changes.is_empty() {
            state.save_entries();
        }
        state.save_lamport();
        Ok(state)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn root_hash(&self) -> [u8; 32] {
        self.tree.root_hash
    }

    pub fn live_root_hash(&self) -> [u8; 32] {
        self.live_tree.root_hash
    }

    pub fn tree(&self) -> &TreeSnapshot {
        &self.tree
    }

    pub fn live_tree(&self) -> &TreeSnapshot {
        &self.live_tree
    }

    pub fn lamport(&self) -> u64 {
        self.lamport
    }

    pub fn gc_watermark(&self) -> &GcWatermark {
        &self.gc_watermark
    }

    pub fn node(&self, prefix: &str) -> Option<TreeNode> {
        self.tree.nodes.get(&normalize_prefix(prefix)).cloned()
    }

    pub fn entry(&self, path: &str) -> Option<Entry> {
        self.entries.get(path.trim_matches('/')).cloned()
    }

    pub fn object_file(&self, content_hash: [u8; 32]) -> Option<(PathBuf, u64)> {
        self.entries.values().find_map(|entry| {
            if entry.kind == EntryKind::File && entry.content_hash == Some(content_hash) {
                Some((self.root.join(&entry.path), entry.size))
            } else {
                None
            }
        })
    }

    pub fn should_accept_remote(&self, remote: &Entry) -> bool {
        if validate_remote_path(&remote.path).is_err() {
            return false;
        }
        // The GC watermark prevents re-applying tombstones that have already been
        // cleaned up. It is intentionally not applied to live entries: the
        // per-origin max-lamport watermark is too broad and would incorrectly
        // block legitimate live files from the same origin that predate the
        // watermark. For the trusted-peer model lil targets, this is acceptable.
        if remote.kind == EntryKind::Tombstone && self.is_version_gced(&remote.version) {
            return false;
        }
        self.entries
            .get(&remote.path)
            .map(|local| remote.version > local.version)
            .unwrap_or(true)
    }

    pub fn tmp_recv_path(&self, entry: &Entry) -> PathBuf {
        let id = NEXT_RECV_ID.fetch_add(1, AtomicOrdering::Relaxed);
        self.root.join(STATE_DIR).join(format!(
            "recv-{}-{}-{}-{}",
            std::process::id(),
            id,
            entry.version.lamport,
            entry.path.replace('/', "_")
        ))
    }

    /// Apply a remote entry, returning every change it produced: the entry
    /// itself plus any descendant tombstones created when a file or symlink
    /// replaces a directory. Returns an empty vec if the entry was not accepted.
    pub fn apply_remote_entry(
        &mut self,
        remote: Entry,
        object_tmp_path: Option<&Path>,
    ) -> io::Result<Vec<Change>> {
        validate_remote_path(&remote.path)?;
        if !self.should_accept_remote(&remote) {
            if let Some(tmp) = object_tmp_path {
                let _ = fs::remove_file(tmp);
            }
            return Ok(Vec::new());
        }

        match remote.kind {
            EntryKind::File => {
                let tmp = object_tmp_path.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing tmp path for file {}", remote.path),
                    )
                })?;
                self.install_remote_file(&remote, tmp)?;
            }
            EntryKind::Dir => {
                self.install_remote_dir(&remote)?;
            }
            EntryKind::Symlink => {
                self.install_remote_symlink(&remote)?;
            }
            EntryKind::Tombstone => {
                remove_path_if_exists(&self.root.join(&remote.path))?;
            }
        }

        let path = remote.path.clone();
        self.lamport = self.lamport.max(remote.version.lamport);
        let old = self.entries.insert(path.clone(), remote.clone());

        let mut extra_changes = Vec::new();
        if remote.kind != EntryKind::Dir {
            self.tombstone_child_descendants(&path, &mut extra_changes);
        }
        self.save_lamport();
        self.save_entries();

        let mut changed_paths: Vec<String> = vec![path.clone()];
        changed_paths.extend(extra_changes.iter().map(|c| c.path.clone()));
        update_tree_snapshot(&mut self.tree, &self.entries, &changed_paths, |_| true);
        update_tree_snapshot(&mut self.live_tree, &self.entries, &changed_paths, |e| {
            e.kind != EntryKind::Tombstone
        });

        let mut all_changes = vec![Change {
            path,
            old,
            new: remote,
        }];
        all_changes.extend(extra_changes);
        Ok(all_changes)
    }

    fn install_remote_file(&self, remote: &Entry, tmp_path: &Path) -> io::Result<()> {
        let dest = self.root.join(&remote.path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        remove_path_if_exists(&dest)?;
        fs::rename(tmp_path, &dest)?;

        #[cfg(unix)]
        if let Some(m) = sanitize_remote_mode(remote.mode) {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&dest, fs::Permissions::from_mode(m))?;
        }

        Ok(())
    }

    fn install_remote_dir(&self, remote: &Entry) -> io::Result<()> {
        let dest = self.root.join(&remote.path);
        install_remote_dir(&dest)?;
        set_mode(&dest, sanitize_remote_mode(remote.mode))
    }

    fn install_remote_symlink(&self, remote: &Entry) -> io::Result<()> {
        let dest = self.root.join(&remote.path);
        let target = remote.symlink_target.as_deref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("missing symlink target for {}", remote.path),
            )
        })?;
        validate_symlink_target(target)?;
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        remove_path_if_exists(&dest)?;
        create_symlink(target, &dest)
    }

    pub fn rescan(&mut self) -> io::Result<Vec<Change>> {
        let ignore_patterns = load_ignore_patterns(&self.root)?;
        let live = scan_folder(&self.root)?;
        let mut changes = Vec::new();
        let mut seen = live.unstable;

        for (path, live_entry) in live.entries {
            seen.insert(path.clone());
            self.update_entry(&path, live_entry, &mut changes);
        }

        let old_paths: Vec<String> = self.entries.keys().cloned().collect();
        for path in old_paths {
            if seen.contains(&path) {
                continue;
            }
            if should_ignore(&path, &ignore_patterns) {
                continue;
            }
            let Some(old) = self.entries.get(&path).cloned() else {
                continue;
            };
            if old.kind == EntryKind::Tombstone {
                continue;
            }
            let version = self.next_version();
            let new = tombstone_entry(&path, &old, version);
            self.entries.insert(path.clone(), new.clone());
            changes.push(Change {
                path,
                old: Some(old),
                new,
            });
        }

        self.tree = derive_tree(&self.entries);
        self.live_tree = derive_live_tree(&self.entries);
        Ok(changes)
    }

    pub fn apply_paths(&mut self, abs_paths: Vec<PathBuf>) -> io::Result<Vec<Change>> {
        let canonical_paths: Vec<PathBuf> = abs_paths
            .into_iter()
            .filter_map(|p| normalize_event_path(&p))
            .collect();

        if canonical_paths
            .iter()
            .any(|p| p == &self.root.join(IGNORE_FILE_NAME))
        {
            return self.rescan();
        }

        let ignore_patterns = load_ignore_patterns(&self.root)?;
        let mut changes = Vec::new();

        for abs_path in canonical_paths {
            let relative = match relative_path(&self.root, &abs_path) {
                Ok(r) if !r.is_empty() => r,
                _ => continue,
            };
            self.apply_one_path(&abs_path, &relative, &ignore_patterns, &mut changes)?;
        }

        if !changes.is_empty() {
            let changed_paths: Vec<String> = changes.iter().map(|c| c.path.clone()).collect();
            update_tree_snapshot(&mut self.tree, &self.entries, &changed_paths, |_| true);
            update_tree_snapshot(&mut self.live_tree, &self.entries, &changed_paths, |e| {
                e.kind != EntryKind::Tombstone
            });
            self.save_lamport();
            self.save_entries();
        }

        Ok(changes)
    }

    fn apply_one_path(
        &mut self,
        abs_path: &Path,
        relative: &str,
        ignore_patterns: &[crate::ignore::IgnorePattern],
        changes: &mut Vec<Change>,
    ) -> io::Result<()> {
        if should_ignore(relative, ignore_patterns) {
            return Ok(());
        }

        match fs::symlink_metadata(abs_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                if let Some(target) = crate::scan::read_supported_symlink(abs_path)? {
                    let live = Entry {
                        path: relative.to_string(),
                        kind: EntryKind::Symlink,
                        content_hash: None,
                        symlink_target: Some(target),
                        size: 0,
                        mode: None,
                        version: placeholder_version(),
                    };
                    self.update_entry(relative, live, changes);
                }
            }
            Ok(metadata) if metadata.is_file() => {
                let Some(observed) = observe_file_when_stable(abs_path, metadata, true)? else {
                    tracing::debug!("file still changing; skipping {relative}");
                    return Ok(());
                };
                let live = Entry {
                    path: relative.to_string(),
                    kind: EntryKind::File,
                    content_hash: Some(observed.content_hash),
                    symlink_target: None,
                    size: observed.size,
                    mode: observed.mode,
                    version: placeholder_version(),
                };
                self.update_entry(relative, live, changes);
            }
            Ok(metadata) if metadata.is_dir() => {
                let live = Entry {
                    path: relative.to_string(),
                    kind: EntryKind::Dir,
                    content_hash: None,
                    symlink_target: None,
                    size: 0,
                    mode: mode(&metadata),
                    version: placeholder_version(),
                };
                self.update_entry(relative, live, changes);
                let mut dir_entries = BTreeMap::new();
                let mut unstable = std::collections::BTreeSet::new();
                scan_dir(
                    &self.root,
                    abs_path,
                    ignore_patterns,
                    &mut dir_entries,
                    &mut unstable,
                )?;
                for (path, live) in dir_entries {
                    self.update_entry(&path, live, changes);
                }
            }
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                self.tombstone_descendants(relative, changes);
            }
            Err(err) => return Err(err),
        }
        Ok(())
    }

    fn update_entry(&mut self, path: &str, live: Entry, changes: &mut Vec<Change>) {
        let existing = self.entries.get(path).cloned();
        if existing
            .as_ref()
            .is_some_and(|o| same_observed_state(o, &live))
        {
            return;
        }
        let replaces_dir_with_non_dir = existing
            .as_ref()
            .is_some_and(|old| old.kind == EntryKind::Dir)
            && live.kind != EntryKind::Dir;
        let mut new = live;
        new.version = self.next_version();
        self.entries.insert(path.to_string(), new.clone());
        changes.push(Change {
            path: path.to_string(),
            old: existing,
            new,
        });
        if replaces_dir_with_non_dir {
            self.tombstone_child_descendants(path, changes);
        }
    }

    fn tombstone_descendants(&mut self, relative: &str, changes: &mut Vec<Change>) {
        let prefix = format!("{relative}/");
        let to_tombstone: Vec<String> = self
            .entries
            .keys()
            .filter(|p| **p == relative || p.starts_with(&prefix))
            .cloned()
            .collect();
        for path in to_tombstone {
            let old = self.entries.get(&path).cloned().unwrap();
            if old.kind == EntryKind::Tombstone {
                continue;
            }
            let version = self.next_version();
            let new = tombstone_entry(&path, &old, version);
            self.entries.insert(path.clone(), new.clone());
            changes.push(Change {
                path,
                old: Some(old),
                new,
            });
        }
    }

    fn tombstone_child_descendants(&mut self, relative: &str, changes: &mut Vec<Change>) {
        let prefix = format!("{relative}/");
        let to_tombstone: Vec<String> = self
            .entries
            .keys()
            .filter(|p| p.starts_with(&prefix))
            .cloned()
            .collect();
        for path in to_tombstone {
            let old = self.entries.get(&path).cloned().unwrap();
            if old.kind == EntryKind::Tombstone {
                continue;
            }
            let version = self.next_version();
            let new = tombstone_entry(&path, &old, version);
            self.entries.insert(path.clone(), new.clone());
            changes.push(Change {
                path,
                old: Some(old),
                new,
            });
        }
    }

    fn next_version(&mut self) -> Version {
        self.lamport += 1;
        Version {
            lamport: self.lamport,
            origin: self.origin.clone(),
        }
    }

    fn save_lamport(&self) {
        let path = self.root.join(STATE_DIR).join("lamport");
        if let Err(err) = write_atomic(&path, &self.lamport.to_le_bytes()) {
            tracing::warn!("failed to save lamport clock: {err}");
        }
    }

    pub fn save_entries(&self) {
        let path = self.root.join(STATE_DIR).join("entries.bin");
        if let Ok(bytes) = bincode::serialize(&self.entries)
            && let Err(err) = write_atomic(&path, &bytes)
        {
            tracing::warn!("failed to save entries index: {err}");
        }
    }

    pub fn merge_gc_watermark(&mut self, incoming: &GcWatermark) -> (bool, usize) {
        let mut changed = false;
        for (origin, lamport) in incoming {
            let local = self.gc_watermark.entry(origin.clone()).or_insert(0);
            if *lamport > *local {
                *local = *lamport;
                changed = true;
            }
        }
        if changed {
            self.save_gc_watermark();
        }
        let pruned = self.prune_tombstones_covered_by_watermark();
        if pruned > 0 {
            self.lamport += 1;
            self.save_lamport();
        }
        (changed, pruned)
    }

    pub fn gc_tombstones_for_converged_root(&mut self) -> usize {
        let mut watermark = GcWatermark::new();
        for entry in self.entries.values() {
            if entry.kind != EntryKind::Tombstone {
                continue;
            }
            let lamport = watermark.entry(entry.version.origin.clone()).or_insert(0);
            *lamport = (*lamport).max(entry.version.lamport);
        }
        if watermark.is_empty() {
            return 0;
        }
        self.merge_gc_watermark(&watermark).1
    }

    fn is_version_gced(&self, version: &Version) -> bool {
        self.gc_watermark
            .get(&version.origin)
            .is_some_and(|lamport| version.lamport <= *lamport)
    }

    fn prune_tombstones_covered_by_watermark(&mut self) -> usize {
        let to_remove: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, e)| e.kind == EntryKind::Tombstone && self.is_version_gced(&e.version))
            .map(|(k, _)| k.clone())
            .collect();
        let count = to_remove.len();
        if count > 0 {
            for key in &to_remove {
                self.entries.remove(key);
            }
            self.tree = derive_tree(&self.entries);
            self.live_tree = derive_live_tree(&self.entries);
        }
        count
    }

    fn save_gc_watermark(&self) {
        let path = self.root.join(STATE_DIR).join("gc-watermark.bin");
        if let Ok(bytes) = bincode::serialize(&self.gc_watermark)
            && let Err(err) = write_atomic(&path, &bytes)
        {
            tracing::warn!("failed to save gc watermark: {err}");
        }
    }
}

/// Write `bytes` to `path` atomically: write a sibling temp file, then
/// rename over the destination, so readers never observe a truncated file.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, path)
}

fn load_saved_entries(state_dir: &Path) -> io::Result<BTreeMap<String, Entry>> {
    let path = state_dir.join("entries.bin");
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(err) => return Err(err),
    };
    if let Ok(entries) = bincode::deserialize(&bytes) {
        return Ok(entries);
    }
    bincode::deserialize::<BTreeMap<String, LegacyEntry>>(&bytes)
        .map(|entries| {
            entries
                .into_iter()
                .map(|(path, entry)| (path, entry.into_entry()))
                .collect()
        })
        .map_err(|_| {
            io::Error::other(format!(
                "corrupt state index {}: refusing to start with an empty index \
                 (deleting the file rebuilds it from disk, losing deletion history)",
                path.display()
            ))
        })
}

pub fn load_stored_entries(root: &Path) -> io::Result<BTreeMap<String, Entry>> {
    load_saved_entries(&root.join(STATE_DIR))
}

fn cleanup_recv_files(state_dir: &Path) {
    let Ok(entries) = fs::read_dir(state_dir) else {
        return;
    };

    for entry in entries.flatten() {
        if !entry.file_name().to_string_lossy().starts_with("recv-") {
            continue;
        }
        if let Err(err) = remove_path_if_exists(&entry.path()) {
            tracing::debug!(
                "failed to remove stale receive file {:?}: {}",
                entry.path(),
                err
            );
        }
    }
}

fn load_saved_lamport(state_dir: &Path) -> u64 {
    fs::read(state_dir.join("lamport"))
        .ok()
        .and_then(|b| b.try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0)
}

fn load_gc_watermark(state_dir: &Path) -> GcWatermark {
    fs::read(state_dir.join("gc-watermark.bin"))
        .ok()
        .and_then(|bytes| bincode::deserialize(&bytes).ok())
        .unwrap_or_default()
}

fn remove_path_if_exists(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => remove_dir_all_writable(path),
        Ok(_) => fs::remove_file(path),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

#[cfg(unix)]
fn remove_dir_all_writable(path: &Path) -> io::Result<()> {
    make_dir_tree_writable(path)?;
    fs::remove_dir_all(path)
}

#[cfg(not(unix))]
fn remove_dir_all_writable(path: &Path) -> io::Result<()> {
    fs::remove_dir_all(path)
}

#[cfg(unix)]
fn make_dir_tree_writable(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() {
        return Ok(());
    }
    set_mode(path, Some(metadata.permissions().mode() | 0o700))?;
    for child in fs::read_dir(path)? {
        let child = child?;
        let child_path = child.path();
        let child_metadata = fs::symlink_metadata(&child_path)?;
        if child_metadata.is_dir() {
            make_dir_tree_writable(&child_path)?;
        }
    }
    Ok(())
}

fn install_remote_dir(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => {
            remove_path_if_exists(path)?;
            fs::create_dir_all(path)
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => fs::create_dir_all(path),
        Err(err) => Err(err),
    }
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: Option<u32>) -> io::Result<()> {
    let Some(mode) = mode else {
        return Ok(());
    };
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: Option<u32>) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn create_symlink(target: &str, dest: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, dest)
}

#[cfg(not(unix))]
fn create_symlink(_target: &str, _dest: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "symlink replication is not supported on this platform",
    ))
}

/// Strip the setuid/setgid bits from a mode received from a peer before it is
/// applied locally. Even within the trusted-member model, replicating these
/// bits would let a member's data plant setuid/setgid files on every peer; the
/// ordinary permission bits are preserved.
fn sanitize_remote_mode(mode: Option<u32>) -> Option<u32> {
    mode.map(|m| m & !0o6000)
}

fn validate_remote_path(path: &str) -> io::Result<()> {
    use crate::ignore::should_ignore_component;

    if path.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "remote path must not be empty",
        ));
    }
    if path.contains('\0') || path.contains('\\') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid remote path {path:?}"),
        ));
    }

    let mut components = path.split('/');
    if components.any(|component| {
        component.is_empty()
            || component == "."
            || component == ".."
            || should_ignore_component(component)
    }) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid remote path {path:?}"),
        ));
    }

    let path = Path::new(path);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::Prefix(_)
                    | std::path::Component::RootDir
                    | std::path::Component::CurDir
                    | std::path::Component::ParentDir
            )
        })
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid remote path {path:?}"),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn records_create_update_delete_as_versions() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("a.txt");
        fs::write(&file, "one").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let initial_hash = state.root_hash();
        let initial_live_hash = state.live_root_hash();

        fs::write(&file, "two").unwrap();
        let changes = state.rescan().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].verb(), "file update");
        assert_eq!(changes[0].new.version.lamport, 2);
        assert_ne!(state.root_hash(), initial_hash);
        assert_ne!(state.live_root_hash(), initial_live_hash);

        fs::remove_file(&file).unwrap();
        let changes = state.rescan().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].verb(), "delete");
        assert_eq!(changes[0].new.kind, EntryKind::Tombstone);
        assert_eq!(changes[0].new.version.lamport, 3);
        assert_ne!(state.root_hash(), initial_hash);
    }

    #[test]
    fn corrupt_entries_file_is_a_hard_error() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("a.txt"), "one").unwrap();
        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        drop(state);

        let entries_path = tmp.path().join(".lil").join("entries.bin");
        fs::write(&entries_path, b"\xff\xff\xff").unwrap();

        let err = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string());
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("corrupt state index"));
    }

    #[test]
    fn lamport_clock_never_falls_below_loaded_entry_versions() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("a.txt");
        fs::write(&file, "one").unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let max_before = state
            .entries
            .values()
            .map(|e| e.version.lamport)
            .max()
            .unwrap();
        drop(state);

        fs::remove_file(tmp.path().join(".lil").join("lamport")).unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        fs::write(&file, "two").unwrap();
        let changes = state.rescan().unwrap();
        assert_eq!(changes.len(), 1);
        assert!(changes[0].new.version.lamport > max_before);
    }

    #[test]
    fn derives_child_nodes_for_directories() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("src")).unwrap();
        let mut file = fs::File::create(tmp.path().join("src/main.rs")).unwrap();
        writeln!(file, "fn main() {{}}").unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        let root = state.tree().nodes.get("").unwrap();
        assert!(root.entries.contains_key("src"));
        assert!(root.children.contains_key("src"));
        assert!(state.tree().nodes.contains_key("src"));
        assert_ne!(state.root_hash(), [0; 32]);
    }

    #[test]
    fn scan_ignores_state_and_os_metadata_paths() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join(".lil")).unwrap();
        fs::write(tmp.path().join(".lil/private.key"), "secret").unwrap();
        fs::write(tmp.path().join(".DS_Store"), "metadata").unwrap();
        fs::write(tmp.path().join("Thumbs.db"), "metadata").unwrap();
        fs::write(tmp.path().join("Desktop.ini"), "metadata").unwrap();
        fs::write(tmp.path().join("._file.txt"), "metadata").unwrap();
        fs::create_dir(tmp.path().join("lost+found")).unwrap();
        fs::write(tmp.path().join("lost+found/orphan"), "metadata").unwrap();
        fs::create_dir(tmp.path().join(".Spotlight-V100")).unwrap();
        fs::write(tmp.path().join(".Spotlight-V100/store"), "metadata").unwrap();
        fs::write(tmp.path().join("keep.txt"), "keep").unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        assert!(state.entries.contains_key("keep.txt"));
        assert!(!state.entries.contains_key(".lil"));
        assert!(!state.entries.contains_key(".DS_Store"));
        assert!(!state.entries.contains_key("Thumbs.db"));
        assert!(!state.entries.contains_key("Desktop.ini"));
        assert!(!state.entries.contains_key("._file.txt"));
        assert!(
            !state
                .entries
                .keys()
                .any(|path| path.starts_with("lost+found"))
        );
        assert!(
            !state
                .entries
                .keys()
                .any(|path| path.starts_with(".Spotlight-V100"))
        );
    }

    #[test]
    fn scan_respects_nolil_patterns() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join(".nolil"),
            "ignored.txt\nlogs/\n*.tmp\n/sub/exact.txt\n",
        )
        .unwrap();
        fs::write(tmp.path().join("ignored.txt"), "ignored").unwrap();
        fs::write(tmp.path().join("keep.txt"), "keep").unwrap();
        fs::write(tmp.path().join("note.tmp"), "tmp").unwrap();
        fs::create_dir(tmp.path().join("logs")).unwrap();
        fs::write(tmp.path().join("logs/a.txt"), "log").unwrap();
        fs::create_dir(tmp.path().join("sub")).unwrap();
        fs::write(tmp.path().join("sub/exact.txt"), "exact").unwrap();
        fs::write(tmp.path().join("sub/keep.txt"), "keep").unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        assert!(state.entries.contains_key(".nolil"));
        assert!(state.entries.contains_key("keep.txt"));
        assert!(state.entries.contains_key("sub"));
        assert!(state.entries.contains_key("sub/keep.txt"));
        assert!(!state.entries.contains_key("ignored.txt"));
        assert!(!state.entries.contains_key("note.tmp"));
        assert!(!state.entries.contains_key("logs"));
        assert!(!state.entries.contains_key("logs/a.txt"));
        assert!(!state.entries.contains_key("sub/exact.txt"));
    }

    #[cfg(unix)]
    #[test]
    fn scans_symlinks_without_following_them() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("target.txt"), "target").unwrap();
        std::os::unix::fs::symlink("target.txt", tmp.path().join("link.txt")).unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let link = state.entry("link.txt").unwrap();

        assert_eq!(link.kind, EntryKind::Symlink);
        assert_eq!(link.symlink_target.as_deref(), Some("target.txt"));
        assert_eq!(link.content_hash, None);
        assert_eq!(link.size, 0);
    }

    #[cfg(unix)]
    #[test]
    fn applies_higher_version_remote_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let remote = Entry {
            path: "link.txt".to_string(),
            kind: EntryKind::Symlink,
            content_hash: None,
            symlink_target: Some("target.txt".to_string()),
            size: 0,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        let change = state.apply_remote_entry(remote, None).unwrap().remove(0);

        assert_eq!(change.verb(), "symlink new");
        assert_eq!(
            fs::read_link(tmp.path().join("link.txt")).unwrap(),
            PathBuf::from("target.txt")
        );
        assert_eq!(state.entry("link.txt").unwrap().version.lamport, 10);
    }

    #[cfg(unix)]
    #[test]
    fn strips_setuid_setgid_from_remote_file_mode() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let bytes = b"payload\n";
        let remote = Entry {
            path: "tool".to_string(),
            kind: EntryKind::File,
            content_hash: Some(*blake3::hash(bytes).as_bytes()),
            symlink_target: None,
            size: bytes.len() as u64,
            mode: Some(0o6755),
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        let tmp_path = state.tmp_recv_path(&remote);
        fs::write(&tmp_path, bytes).unwrap();
        state.apply_remote_entry(remote, Some(&tmp_path)).unwrap();

        let mode = fs::symlink_metadata(tmp.path().join("tool"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o6000, 0, "setuid/setgid bits must be stripped");
        assert_eq!(mode & 0o777, 0o755, "permission bits must be preserved");
    }

    #[cfg(unix)]
    #[test]
    fn applies_remote_directory_mode() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("remote-dir")).unwrap();
        fs::set_permissions(
            tmp.path().join("remote-dir"),
            fs::Permissions::from_mode(0o775),
        )
        .unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let remote = Entry {
            path: "remote-dir".to_string(),
            kind: EntryKind::Dir,
            content_hash: None,
            symlink_target: None,
            size: 0,
            mode: Some(0o40755),
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        state.apply_remote_entry(remote, None).unwrap();

        let metadata = fs::symlink_metadata(tmp.path().join("remote-dir")).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o755);
        assert!(state.rescan().unwrap().is_empty());
        assert_eq!(state.entry("remote-dir").unwrap().version.lamport, 10);
    }

    #[cfg(unix)]
    #[test]
    fn applies_remote_tombstone_to_read_only_directory_tree() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("remote-dir");
        let child = dir.join("child");
        fs::create_dir_all(&child).unwrap();
        fs::write(child.join("file.txt"), "content").unwrap();
        fs::set_permissions(&child, fs::Permissions::from_mode(0o555)).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let remote = Entry {
            path: "remote-dir".to_string(),
            kind: EntryKind::Tombstone,
            content_hash: None,
            symlink_target: None,
            size: 0,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        let change = state.apply_remote_entry(remote, None).unwrap().remove(0);

        assert_eq!(change.verb(), "delete");
        assert!(!dir.exists());
        assert_eq!(
            state.entry("remote-dir").unwrap().kind,
            EntryKind::Tombstone
        );
        assert_eq!(
            state.entry("remote-dir/child").unwrap().kind,
            EntryKind::Tombstone
        );
        assert_eq!(
            state.entry("remote-dir/child/file.txt").unwrap().kind,
            EntryKind::Tombstone
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_remote_symlink_targets_that_escape() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let remote = Entry {
            path: "link.txt".to_string(),
            kind: EntryKind::Symlink,
            content_hash: None,
            symlink_target: Some("../outside.txt".to_string()),
            size: 0,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        let err = state.apply_remote_entry(remote, None).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(!tmp.path().join("link.txt").exists());
    }

    #[test]
    fn nolil_negation_reincludes_path() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join(".nolil"), "*.tmp\n!keep.tmp\n").unwrap();
        fs::write(tmp.path().join("drop.tmp"), "drop").unwrap();
        fs::write(tmp.path().join("keep.tmp"), "keep").unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        assert!(!state.entries.contains_key("drop.tmp"));
        assert!(state.entries.contains_key("keep.tmp"));
    }

    #[test]
    fn newly_ignored_path_stays_in_state() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("keep.txt"), "keep").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        assert!(state.entries.contains_key("keep.txt"));

        fs::write(tmp.path().join(".nolil"), "keep.txt\n").unwrap();
        let changes = state.rescan().unwrap();

        assert!(changes.iter().any(|c| c.path == ".nolil"));
        let entry = state.entries.get("keep.txt").unwrap();
        assert_eq!(entry.kind, EntryKind::File);
    }

    #[test]
    fn applies_higher_version_remote_file() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let bytes = b"remote\n";
        let remote = Entry {
            path: "remote.txt".to_string(),
            kind: EntryKind::File,
            content_hash: Some(*blake3::hash(bytes).as_bytes()),
            symlink_target: None,
            size: bytes.len() as u64,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        let tmp_path = state.tmp_recv_path(&remote);
        fs::write(&tmp_path, bytes).unwrap();
        let change = state
            .apply_remote_entry(remote, Some(&tmp_path))
            .unwrap()
            .remove(0);

        assert_eq!(change.verb(), "file new");
        assert_eq!(fs::read(tmp.path().join("remote.txt")).unwrap(), bytes);
        assert_eq!(state.entry("remote.txt").unwrap().version.lamport, 10);
        assert_eq!(state.lamport(), 10);
    }

    #[test]
    fn receive_temp_paths_are_unique_per_attempt() {
        let tmp = tempfile::tempdir().unwrap();
        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let entry = Entry {
            path: "large.zip".to_string(),
            kind: EntryKind::File,
            content_hash: Some([1; 32]),
            symlink_target: None,
            size: 100,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        assert_ne!(state.tmp_recv_path(&entry), state.tmp_recv_path(&entry));
    }

    #[test]
    fn object_file_uses_indexed_size_when_live_file_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("large.zip");
        fs::write(&file, "old").unwrap();
        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let entry = state.entry("large.zip").unwrap();
        let hash = entry.content_hash.unwrap();

        fs::write(&file, "old plus more bytes").unwrap();

        let (path, size) = state.object_file(hash).unwrap();
        assert_eq!(path, fs::canonicalize(&file).unwrap());
        assert_eq!(size, entry.size);
        assert_ne!(fs::metadata(path).unwrap().len(), size);
    }

    #[test]
    fn remote_file_replaces_local_directory_and_tombstones_children() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("sub")).unwrap();
        fs::write(tmp.path().join("sub/a.txt"), "local").unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let bytes = b"remote file\n";
        let remote = Entry {
            path: "sub".to_string(),
            kind: EntryKind::File,
            content_hash: Some(*blake3::hash(bytes).as_bytes()),
            symlink_target: None,
            size: bytes.len() as u64,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        let tmp_path = state.tmp_recv_path(&remote);
        fs::write(&tmp_path, bytes).unwrap();
        let changes = state.apply_remote_entry(remote, Some(&tmp_path)).unwrap();

        // The descendant tombstone must be reported alongside the file entry,
        // not silently dropped.
        assert!(changes.iter().any(|c| c.path == "sub" && c.new.kind == EntryKind::File));
        assert!(
            changes
                .iter()
                .any(|c| c.path == "sub/a.txt" && c.new.kind == EntryKind::Tombstone)
        );
        assert_eq!(fs::read(tmp.path().join("sub")).unwrap(), bytes);
        assert_eq!(state.entry("sub").unwrap().kind, EntryKind::File);
        assert_eq!(state.entry("sub/a.txt").unwrap().kind, EntryKind::Tombstone);
    }

    #[test]
    fn remote_directory_replaces_local_file() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("sub"), "local").unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let remote = Entry {
            path: "sub".to_string(),
            kind: EntryKind::Dir,
            content_hash: None,
            symlink_target: None,
            size: 0,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        state.apply_remote_entry(remote, None).unwrap();

        assert!(tmp.path().join("sub").is_dir());
        assert_eq!(state.entry("sub").unwrap().kind, EntryKind::Dir);
    }

    #[test]
    fn rejects_remote_paths_outside_sync_root() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let bytes = b"remote\n";
        let remote = Entry {
            path: "../outside.txt".to_string(),
            kind: EntryKind::File,
            content_hash: Some(*blake3::hash(bytes).as_bytes()),
            symlink_target: None,
            size: bytes.len() as u64,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };
        let tmp_path = tmp.path().join(".lil").join("recv-path-test");
        fs::write(&tmp_path, bytes).unwrap();

        assert!(!state.should_accept_remote(&remote));
        let err = state
            .apply_remote_entry(remote, Some(&tmp_path))
            .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(!tmp.path().join("..").join("outside.txt").exists());
    }

    #[test]
    fn rejects_remote_paths_inside_state_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let remote = Entry {
            path: ".lil/private.key".to_string(),
            kind: EntryKind::Tombstone,
            content_hash: None,
            symlink_target: None,
            size: 0,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        assert!(!state.should_accept_remote(&remote));
        let err = state.apply_remote_entry(remote, None).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_absolute_and_malformed_remote_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        for path in [
            "/tmp/outside.txt",
            "sub//file.txt",
            "sub/./file.txt",
            r"sub\file.txt",
        ] {
            let remote = Entry {
                path: path.to_string(),
                kind: EntryKind::Tombstone,
                content_hash: None,
                symlink_target: None,
                size: 0,
                mode: None,
                version: Version {
                    lamport: 10,
                    origin: "node-b".to_string(),
                },
            };

            assert!(!state.should_accept_remote(&remote));
        }
    }

    #[test]
    fn applies_higher_version_remote_tombstone() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("gone.txt"), "local").unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let remote = Entry {
            path: "gone.txt".to_string(),
            kind: EntryKind::Tombstone,
            content_hash: None,
            symlink_target: None,
            size: 0,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        let change = state.apply_remote_entry(remote, None).unwrap().remove(0);

        assert_eq!(change.verb(), "delete");
        assert!(!tmp.path().join("gone.txt").exists());
        assert_eq!(state.entry("gone.txt").unwrap().kind, EntryKind::Tombstone);
    }

    #[test]
    fn apply_paths_handles_create_modify_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let empty_hash = state.root_hash();

        let file = tmp.path().join("a.txt");
        fs::write(&file, "hello").unwrap();
        let changes = state.apply_paths(vec![file.clone()]).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].verb(), "file new");
        assert_ne!(state.root_hash(), empty_hash);

        fs::write(&file, "world").unwrap();
        let changes = state.apply_paths(vec![file.clone()]).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].verb(), "file update");

        fs::remove_file(&file).unwrap();
        let changes = state.apply_paths(vec![file]).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].verb(), "delete");
        assert_eq!(changes[0].new.kind, EntryKind::Tombstone);
    }

    #[test]
    fn apply_paths_tombstones_dir_descendants_on_removal() {
        let tmp = tempfile::tempdir().unwrap();
        let sub = tmp.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("a.txt"), "a").unwrap();
        fs::write(sub.join("b.txt"), "b").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        assert_eq!(
            state.entries.get("sub/a.txt").unwrap().kind,
            EntryKind::File
        );

        fs::remove_dir_all(&sub).unwrap();
        let changes = state.apply_paths(vec![sub]).unwrap();
        assert!(changes.len() >= 3); // sub, sub/a.txt, sub/b.txt
        for c in &changes {
            assert_eq!(c.new.kind, EntryKind::Tombstone);
        }
    }

    #[test]
    fn apply_paths_scans_new_directory_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        let sub = tmp.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("c.txt"), "c").unwrap();

        let changes = state.apply_paths(vec![sub]).unwrap();
        let paths: Vec<&str> = changes.iter().map(|c| c.path.as_str()).collect();
        assert!(paths.contains(&"sub"));
        assert!(paths.contains(&"sub/c.txt"));
    }

    #[test]
    fn apply_paths_tombstones_children_when_file_replaces_directory() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("sub")).unwrap();
        fs::write(tmp.path().join("sub/a.txt"), "local").unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        fs::remove_dir_all(tmp.path().join("sub")).unwrap();
        fs::write(tmp.path().join("sub"), "replacement").unwrap();
        let changes = state.apply_paths(vec![tmp.path().join("sub")]).unwrap();

        assert!(
            changes
                .iter()
                .any(|c| c.path == "sub" && c.new.kind == EntryKind::File)
        );
        assert!(
            changes
                .iter()
                .any(|c| c.path == "sub/a.txt" && c.new.kind == EntryKind::Tombstone)
        );
        assert_eq!(state.entry("sub/a.txt").unwrap().kind, EntryKind::Tombstone);
    }

    #[test]
    fn apply_paths_falls_back_to_rescan_when_nolil_changes() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("keep.txt"), "keep").unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        let nolil = tmp.path().join(".nolil");
        fs::write(&nolil, "keep.txt\n").unwrap();
        let changes = state.apply_paths(vec![nolil]).unwrap();

        assert!(changes.iter().any(|c| c.path == ".nolil"));
        assert_eq!(state.entries.get("keep.txt").unwrap().kind, EntryKind::File);
    }

    #[test]
    fn incremental_tree_matches_full_rebuild() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("sub")).unwrap();
        fs::write(tmp.path().join("sub/a.txt"), "initial").unwrap();
        fs::write(tmp.path().join("b.txt"), "b").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        fs::write(tmp.path().join("sub/a.txt"), "modified").unwrap();
        state
            .apply_paths(vec![tmp.path().join("sub/a.txt")])
            .unwrap();
        let incremental_root = state.root_hash();
        let incremental_live = state.live_root_hash();

        let further_changes = state.rescan().unwrap();
        assert!(
            further_changes.is_empty(),
            "rescan found unexpected changes after apply_paths"
        );
        assert_eq!(state.root_hash(), incremental_root);
        assert_eq!(state.live_root_hash(), incremental_live);

        fs::remove_file(tmp.path().join("b.txt")).unwrap();
        state.apply_paths(vec![tmp.path().join("b.txt")]).unwrap();
        let after_delete_root = state.root_hash();
        let after_delete_live = state.live_root_hash();

        let further_changes = state.rescan().unwrap();
        assert!(
            further_changes.is_empty(),
            "rescan found unexpected changes after delete"
        );
        assert_eq!(state.root_hash(), after_delete_root);
        assert_eq!(state.live_root_hash(), after_delete_live);
    }

    #[test]
    fn tombstone_survives_restart_and_converged_gc_clears_it() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("a.txt"), "hello").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        assert_eq!(state.entry("a.txt").unwrap().kind, EntryKind::File);

        fs::remove_file(tmp.path().join("a.txt")).unwrap();
        state.apply_paths(vec![tmp.path().join("a.txt")]).unwrap();
        assert_eq!(state.entry("a.txt").unwrap().kind, EntryKind::Tombstone);

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        assert_eq!(state.entry("a.txt").unwrap().kind, EntryKind::Tombstone);

        assert_eq!(state.gc_tombstones_for_converged_root(), 1);
        assert!(state.entry("a.txt").is_none());
    }

    #[test]
    fn startup_rescan_persists_offline_changes() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("a.txt"), "one").unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        assert_eq!(state.entry("a.txt").unwrap().version.lamport, 1);
        drop(state);

        fs::write(tmp.path().join("a.txt"), "two").unwrap();
        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let offline_version = state.entry("a.txt").unwrap().version;
        assert_eq!(offline_version.lamport, 2);
        drop(state);

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        assert_eq!(state.entry("a.txt").unwrap().version, offline_version);
    }

    #[test]
    fn gc_watermark_survives_restart_and_rejects_stale_versions() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("a.txt"), "hello").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        fs::remove_file(tmp.path().join("a.txt")).unwrap();
        state.apply_paths(vec![tmp.path().join("a.txt")]).unwrap();
        let tombstone = state.entry("a.txt").unwrap();
        let tombstone_version = tombstone.version.clone();

        assert_eq!(state.gc_tombstones_for_converged_root(), 1);
        state.save_entries();
        assert!(state.entry("a.txt").is_none());
        assert_eq!(
            state.gc_watermark().get("node-a").copied(),
            Some(tombstone_version.lamport)
        );

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        assert!(state.entry("a.txt").is_none());
        assert!(!state.should_accept_remote(&tombstone));
    }

    #[test]
    fn live_root_returns_to_initial_after_transient_path_is_removed() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("kept.txt"), "kept").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let initial_state_hash = state.root_hash();
        let initial_live_hash = state.live_root_hash();

        fs::create_dir(tmp.path().join("new")).unwrap();
        fs::write(tmp.path().join("new/a"), "temp").unwrap();
        state.rescan().unwrap();

        fs::remove_file(tmp.path().join("new/a")).unwrap();
        fs::remove_dir(tmp.path().join("new")).unwrap();
        state.rescan().unwrap();

        assert_ne!(state.root_hash(), initial_state_hash);
        assert_eq!(state.live_root_hash(), initial_live_hash);
    }
}
