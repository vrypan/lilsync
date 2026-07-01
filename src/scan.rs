//! Filesystem scanning: recursive directory traversal, file stability
//! detection (hash-after-stable-mtime), symlink reading, and event-path
//! canonicalisation for incremental updates.
//!
//! A local, best-effort size+mtime cache (`.lil/scan-cache.bin`) lets
//! rescans skip re-hashing files whose stat data is unchanged.

use crate::entries::{Entry, EntryKind, placeholder_version, validate_symlink_target};
use crate::ignore::{IgnorePattern, load_ignore_patterns, should_ignore};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const FILE_STABLE_AGE: Duration = Duration::from_secs(1);
pub(crate) const FILE_STABILITY_PAUSE: Duration = Duration::from_millis(250);
pub(crate) const FILE_STABILITY_ATTEMPTS: usize = 8;

const SCAN_CACHE_FILE: &str = "scan-cache.bin";

pub(crate) struct ScanResult {
    pub(crate) entries: BTreeMap<String, Entry>,
    pub(crate) unstable: BTreeSet<String>,
}

pub(crate) struct ObservedFile {
    pub(crate) content_hash: [u8; 32],
    pub(crate) size: u64,
    pub(crate) mode: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CachedFile {
    size: u64,
    mtime: SystemTime,
    content_hash: [u8; 32],
}

/// Local cache of content hashes keyed by relative path and validated by
/// size + mtime, so unchanged files are not re-read on every rescan. Purely
/// an optimisation: it never crosses the wire, and losing or corrupting it
/// only costs re-hashing. Files modified within the last `FILE_STABLE_AGE`
/// are never served from the cache (mtime granularity could hide a write in
/// progress).
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct ScanCache {
    files: BTreeMap<String, CachedFile>,
    #[serde(skip)]
    dirty: bool,
}

impl ScanCache {
    pub(crate) fn load(state_dir: &Path) -> Self {
        fs::read(state_dir.join(SCAN_CACHE_FILE))
            .ok()
            .and_then(|bytes| bincode::deserialize(&bytes).ok())
            .unwrap_or_default()
    }

    /// Best-effort save; skipped when nothing changed since the last save.
    pub(crate) fn save(&mut self, state_dir: &Path) {
        if !self.dirty {
            return;
        }
        if let Ok(bytes) = bincode::serialize(self) {
            match crate::state::write_atomic(&state_dir.join(SCAN_CACHE_FILE), &bytes) {
                Ok(()) => self.dirty = false,
                Err(err) => tracing::debug!("failed to save scan cache: {err}"),
            }
        }
    }

    /// Drop cached paths that no longer exist on disk.
    pub(crate) fn retain_paths(&mut self, seen: &BTreeSet<String>) {
        let before = self.files.len();
        self.files.retain(|path, _| seen.contains(path));
        if self.files.len() != before {
            self.dirty = true;
        }
    }

    fn lookup(&self, relative: &str, metadata: &fs::Metadata) -> Option<[u8; 32]> {
        if is_recently_modified(metadata) {
            return None;
        }
        let cached = self.files.get(relative)?;
        let mtime = metadata.modified().ok()?;
        (cached.size == metadata.len() && cached.mtime == mtime).then_some(cached.content_hash)
    }

    fn record(&mut self, relative: &str, size: u64, mtime: Option<SystemTime>, hash: [u8; 32]) {
        let Some(mtime) = mtime else {
            return;
        };
        self.files.insert(
            relative.to_string(),
            CachedFile {
                size,
                mtime,
                content_hash: hash,
            },
        );
        self.dirty = true;
    }
}

/// Observe a file, serving the content hash from the scan cache when its
/// size and mtime are unchanged, and recording fresh observations back into
/// the cache.
pub(crate) fn observe_file_cached(
    path: &Path,
    relative: &str,
    metadata: fs::Metadata,
    wait_for_recent: bool,
    cache: &mut ScanCache,
) -> io::Result<Option<ObservedFile>> {
    if let Some(content_hash) = cache.lookup(relative, &metadata) {
        return Ok(Some(ObservedFile {
            content_hash,
            size: metadata.len(),
            mode: mode(&metadata),
        }));
    }
    let observed = observe_file_when_stable(path, metadata, wait_for_recent)?;
    if let Some(observed) = &observed {
        let mtime = fs::symlink_metadata(path).ok().and_then(|m| m.modified().ok());
        cache.record(relative, observed.size, mtime, observed.content_hash);
    }
    Ok(observed)
}

pub(crate) fn scan_folder(root: &Path, cache: &mut ScanCache) -> io::Result<ScanResult> {
    let mut entries = BTreeMap::new();
    let mut unstable = BTreeSet::new();
    let ignore_patterns = load_ignore_patterns(root)?;
    scan_dir(root, root, &ignore_patterns, &mut entries, &mut unstable, cache)?;
    Ok(ScanResult { entries, unstable })
}

pub(crate) fn scan_dir(
    root: &Path,
    dir: &Path,
    ignore_patterns: &[IgnorePattern],
    entries: &mut BTreeMap<String, Entry>,
    unstable: &mut BTreeSet<String>,
    cache: &mut ScanCache,
) -> io::Result<()> {
    let mut children = Vec::new();
    for child in fs::read_dir(dir)? {
        let child = child?;
        children.push(child.path());
    }
    children.sort();

    for path in children {
        let relative = relative_path(root, &path)?;
        if should_ignore(&relative, ignore_patterns) {
            continue;
        }

        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            if let Some(target) = read_supported_symlink(&path)? {
                entries.insert(
                    relative.clone(),
                    Entry {
                        path: relative,
                        kind: EntryKind::Symlink,
                        content_hash: None,
                        symlink_target: Some(target),
                        size: 0,
                        mode: None,
                        version: placeholder_version(),
                    },
                );
            }
            continue;
        }

        let file_mode = mode(&metadata);
        if metadata.is_dir() {
            entries.insert(
                relative.clone(),
                Entry {
                    path: relative,
                    kind: EntryKind::Dir,
                    content_hash: None,
                    symlink_target: None,
                    size: 0,
                    mode: file_mode,
                    version: placeholder_version(),
                },
            );
            scan_dir(root, &path, ignore_patterns, entries, unstable, cache)?;
        } else if metadata.is_file() {
            let Some(observed) = observe_file_cached(&path, &relative, metadata, false, cache)?
            else {
                tracing::debug!("file still changing; skipping {relative}");
                unstable.insert(relative);
                continue;
            };
            entries.insert(
                relative.clone(),
                Entry {
                    path: relative,
                    kind: EntryKind::File,
                    content_hash: Some(observed.content_hash),
                    symlink_target: None,
                    size: observed.size,
                    mode: observed.mode,
                    version: placeholder_version(),
                },
            );
        }
    }

    Ok(())
}

pub(crate) fn read_supported_symlink(path: &Path) -> io::Result<Option<String>> {
    let target = fs::read_link(path)?;
    let Some(target) = target.to_str().map(|s| s.to_string()) else {
        return Ok(None);
    };
    if validate_symlink_target(&target).is_err() {
        return Ok(None);
    }
    Ok(Some(target))
}

pub(crate) fn observe_file_when_stable(
    path: &Path,
    mut before: fs::Metadata,
    wait_for_recent: bool,
) -> io::Result<Option<ObservedFile>> {
    for attempt in 0..FILE_STABILITY_ATTEMPTS {
        if wait_for_recent && is_recently_modified(&before) {
            std::thread::sleep(FILE_STABILITY_PAUSE);
            let after_pause = fs::symlink_metadata(path)?;
            if !after_pause.is_file() {
                return Ok(None);
            }
            before = after_pause;
            continue;
        }

        let content_hash = hash_file(path)?;
        let after_hash = fs::symlink_metadata(path)?;
        if !after_hash.is_file() {
            return Ok(None);
        }
        if same_file_observation(&before, &after_hash) {
            return Ok(Some(ObservedFile {
                content_hash,
                size: after_hash.len(),
                mode: mode(&after_hash),
            }));
        }

        before = after_hash;
        if attempt + 1 < FILE_STABILITY_ATTEMPTS {
            std::thread::sleep(FILE_STABILITY_PAUSE);
        }
    }
    Ok(None)
}

fn hash_file(path: &Path) -> io::Result<[u8; 32]> {
    let mut file = fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(*hasher.finalize().as_bytes())
}

fn same_file_observation(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
        && mode(left) == mode(right)
}

fn is_recently_modified(metadata: &fs::Metadata) -> bool {
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    SystemTime::now()
        .duration_since(modified)
        .map(|age| age < FILE_STABLE_AGE)
        .unwrap_or(false)
}

#[cfg(unix)]
pub(crate) fn mode(metadata: &fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    Some(metadata.permissions().mode())
}

#[cfg(not(unix))]
pub(crate) fn mode(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

pub(crate) fn relative_path(root: &Path, path: &Path) -> io::Result<String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

pub(crate) fn normalize_event_path(path: &Path) -> Option<PathBuf> {
    if path.parent().is_none() {
        return fs::canonicalize(path).ok();
    }
    path.parent()
        .and_then(|parent| fs::canonicalize(parent).ok())
        .map(|parent| parent.join(path.file_name().unwrap_or_default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_aged_file(path: &Path, contents: &str) -> fs::Metadata {
        fs::write(path, contents).unwrap();
        let aged = SystemTime::now() - Duration::from_secs(10);
        fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(aged)
            .unwrap();
        fs::symlink_metadata(path).unwrap()
    }

    #[test]
    fn cache_serves_hash_for_unchanged_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("a.txt");
        let metadata = write_aged_file(&path, "hello");

        let poisoned = [7u8; 32];
        let mut cache = ScanCache::default();
        cache.record(
            "a.txt",
            metadata.len(),
            metadata.modified().ok(),
            poisoned,
        );

        let observed = observe_file_cached(&path, "a.txt", metadata, false, &mut cache)
            .unwrap()
            .unwrap();
        // The poisoned hash coming back proves the file was not re-read.
        assert_eq!(observed.content_hash, poisoned);
    }

    #[test]
    fn cache_miss_rehashes_and_records() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("a.txt");
        let metadata = write_aged_file(&path, "hello");
        let real = *blake3::hash(b"hello").as_bytes();

        let mut cache = ScanCache::default();
        let observed = observe_file_cached(&path, "a.txt", metadata, false, &mut cache)
            .unwrap()
            .unwrap();
        assert_eq!(observed.content_hash, real);

        // The fresh observation was recorded: a second call is a cache hit.
        let metadata = fs::symlink_metadata(&path).unwrap();
        assert_eq!(cache.lookup("a.txt", &metadata), Some(real));
    }

    #[test]
    fn recently_modified_files_bypass_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("a.txt");
        fs::write(&path, "hello").unwrap();
        let metadata = fs::symlink_metadata(&path).unwrap();

        let mut cache = ScanCache::default();
        cache.record(
            "a.txt",
            metadata.len(),
            metadata.modified().ok(),
            [7u8; 32],
        );

        // mtime is current, so the poisoned cache entry must be ignored and
        // the file re-hashed.
        let observed = observe_file_cached(&path, "a.txt", metadata, false, &mut cache)
            .unwrap()
            .unwrap();
        assert_eq!(observed.content_hash, *blake3::hash(b"hello").as_bytes());
    }

    #[test]
    fn cache_roundtrips_through_disk_and_prunes_missing_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("a.txt");
        let metadata = write_aged_file(&path, "hello");
        let hash = [9u8; 32];

        let mut cache = ScanCache::default();
        cache.record("a.txt", metadata.len(), metadata.modified().ok(), hash);
        cache.record("gone.txt", 3, metadata.modified().ok(), [1u8; 32]);
        cache.save(tmp.path());

        let mut loaded = ScanCache::load(tmp.path());
        assert_eq!(loaded.lookup("a.txt", &metadata), Some(hash));

        let seen: BTreeSet<String> = [String::from("a.txt")].into();
        loaded.retain_paths(&seen);
        assert!(loaded.files.contains_key("a.txt"));
        assert!(!loaded.files.contains_key("gone.txt"));
    }
}
