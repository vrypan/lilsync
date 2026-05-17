# lilsync Status

Current status: functional Rust daemon using directory-tree state hashes,
iroh-gossip, and custom iroh RPC for reconciliation.

## Implemented

### Folder State Model

- `FolderState` tracks the current folder as a canonical path index: `path -> Entry`.
- Entries support `file`, `dir`, and `tombstone` states.
- Entry versions use Lamport timestamps plus origin node ID.
- Version ordering follows the intended rule: higher Lamport wins; ties are broken by origin.
- File content is hashed with BLAKE3 and stored as `content_hash`.
- Directory and file metadata that should not affect sync, such as mtime, is excluded from hashes.

Degree: functional in memory, reconstructed from the folder on startup. The persistent index/change log/object store from `NEXT.md` is not implemented yet.

### Derived Tree Hashing

- The tree is derived from the entry index.
- Each directory is represented as a tree node.
- Node hashes include sorted entry hashes and sorted child hashes.
- The state root represents the complete state including tombstones.
- A separate live root excludes tombstones so the current visible filesystem can be compared independently from tombstone history.

Degree: functional. The structure is a directory trie, not a compressed radix tree. Incremental rehashing is implemented: after a local or remote change, only the ancestor nodes of the changed paths are recomputed. Full rebuilds are used only on startup and full rescan.

### Folder Monitoring

- `lilsync start <path>` / `lilsync watch <path>` monitors a folder and logs local tree changes.
- Native filesystem notifications are used by default through `notify`.
- `--poll` is available as a fallback.
- Changes are debounced by `--interval-ms`.
- `.lil/` is always ignored.
- `.nolil` ignore rules are supported, including negation.
- Common filesystem noise is ignored, including `.DS_Store` and `lost+found/`.

Degree: functional for local monitoring. The watcher collects affected paths across the debounce window and applies them via `apply_paths`, which stats only the changed paths rather than scanning the full tree. Falls back to a full rescan when `.nolil` changes. Polling mode still triggers full rescans.

### Local Daemon State

- Node private key is persisted at `<folder>/.lil/private.key`.
- The same node ID is reused across restarts.
- `<folder>/.lil/daemon.lock` prevents two instances from running on the same folder.
- Group state is persisted at `<folder>/.lil/peers.json`.
- Pending invite secrets are persisted at `<folder>/.lil/invites.json`.

Degree: functional. There is no migration layer for older state formats.

### Gossip Layer

- Uses `iroh-gossip`.
- Nodes publish periodic `SyncState` messages with root hashes, Lamport, and node counts.
- Nodes publish real-time `FilesystemChanged` messages after local folder changes.
  These messages gossip roots plus a bounded tree hint to reduce follow-up RPCs.
- Nodes publish `Peers` messages containing the known membership ledger.
- Received peer lists are merged and active peers are passed to `GossipSender::join_peers`.
- `SyncState` messages include GC watermarks so peers can prune tombstones and reject stale entries consistently.

Degree: functional for basic peer discovery and event propagation. Gossip messages are not authenticated beyond the iroh node identity of the connection layer, and there is no replay protection or membership signature scheme.

### Persistent Entry State

- All tracked entries (including tombstones) are persisted to `<folder>/.lil/entries.bin` in bincode format.
- On startup, persisted entries are loaded before the filesystem scan so tombstones survive restarts and deleted files are not re-synced.
- Entry state is written after every watcher batch (`apply_paths`) and after each reconciliation pass.
- GC watermarks are persisted to `<folder>/.lil/gc-watermark.bin`.
- Tombstone GC advances only after all active peers report the same state root. The node records a per-origin watermark for tombstones covered by that converged root, prunes them, and gossips the watermark through `SyncState`.
- Entries at or below a known GC watermark are rejected so stale tombstones or files are not resurrected by lagging peers.

Degree: functional. GC is conservative — a tombstone is only removed when all active peers have reported the same state root. Offline active peers delay pruning until they return and sync, or until they are removed from membership.

### Minimal RPC API

Implemented over a custom `lil/rpc/1` ALPN:

- `GetRoot`
- `GetNode(prefix)`
- `GetEntry(path)`
- `GetObject(content_hash)`
- `Join`

RPC connections are reused per peer, with one bidirectional stream per request.

Degree: functional. Tree/file RPCs are gated to active group members. The join RPC is intentionally allowed for non-members presenting a valid invite.

### Tree Reconciliation and File Fetching

- On receiving a remote root mismatch, a node probes the remote peer over RPC.
- Reconciliation descends changed tree nodes by comparing hashes.
- `FilesystemChanged` gossip can provide validated tree nodes as hints, avoiding some `GetNode` RPCs while staying capped below the gossip message budget.
- Changed entries are resolved by version ordering.
- Remote files are fetched by `content_hash` through `GetObject`.
- Remote directories and tombstones are applied locally.
- File bytes are verified against the advertised content hash before being applied.

Degree: functional for pull-based reconciliation. File objects are transferred as raw bytes over the existing QUIC stream: the server streams directly from disk using `tokio::io::copy` with no full-file buffer; the client streams to a tmp file in 256 KB chunks with incremental BLAKE3 verification, then renames into place. Up to 8 file downloads run in parallel per reconciliation pass using `tokio::task::JoinSet` and a `Semaphore`. A post-reconciliation sweep re-runs `tombstone_descendants` on every accepted tombstone path to catch entries installed concurrently during the reconciliation window. Conflict handling beyond deterministic version ordering is not implemented. The change-log fast path is not implemented.

Tombstone correctness guarantees:

- Ignored paths (`.nolil`) never produce tombstones; local ignore rules do not propagate deletions to peers.
- A directory tombstone applied from a remote peer recursively tombstones all live child entries and deletes the directory tree from disk.
- `tombstone_descendants` (local delete path) covers all entries currently in state; the post-reconciliation sweep closes the window for entries added after the tombstone was first applied.

### Group Joining

- Any node can create a one-time ticket with:

```text
lilsync invite <folder>
```

- Tickets are a compact 86-character base62 string encoding the 32-byte issuer node ID and 32-byte secret concatenated.
- Secrets are stored hashed and expire after `--expire-secs`.
- A joining node uses:

```text
lilsync join <folder> <86-char-base62-ticket>
```

- The issuer validates and consumes the ticket.
- The issuer adds the joiner to the peer ledger.
- The join response returns the gossip topic ID and full peer list.
- `lilsync join ... --exit` writes the joined group state and exits instead of starting the daemon, for service-manager setup.
- The issuer broadcasts the updated peer list through gossip.
- Other peers merge newer member entries by Lamport.

Degree: functional for adding and removing peers. Peers can announce a human-readable `--name`; names propagate via the Join RPC and Peers gossip. Any peer can remove another with `lilsync remove <folder> <id|name>`; the change persists to `peers.json` and broadcasts on next daemon start.

## Verified

- Unit tests cover state hashing, tombstones, ignore rules, key reuse, daemon locking, invite consumption, peer merge behavior, gossip message serialization, remote apply behavior, tombstone persistence across restarts, GC watermarks, and bounded tree hints.
- Latest verification run:

```text
cargo fmt -- --check
cargo check
cargo test
cargo clippy --all-targets -- -D warnings
```

- Manual smoke tests verified:
  - local folder monitoring
  - two-node gossip startup
  - invite-based join
  - peer-list gossip
  - tree reconciliation
  - content fetch from one folder to another

## Known Gaps

- No change-log synchronization fast path.
- No push reconciliation; current sync is pull-based when a remote mismatch is observed.
- No explicit conflict file strategy when concurrent versions tie in unexpected ways.
- No peer revocation workflow (removal is trust-based; no signed membership ledger).
- No signed membership ledger.
- No application-level content encryption beyond iroh transport encryption, and no authorization beyond iroh node identity plus local peer membership.
- No long-running integration test harness yet.
- NAT traversal warnings from iroh/quinn can still appear on new connections; current RPC connection reuse reduces repeated RPC warnings but does not eliminate the underlying warning.

## Current Usage

Start the daemon in the background (creates a new group on first run):

```text
lilsync start ./tmp1
lilsync start ./tmp1 --name alice --poll
```

Run in the foreground (useful for debugging):

```text
lilsync watch ./tmp1
lilsync watch ./tmp1 --status
```

Stop the background daemon:

```text
lilsync stop ./tmp1
```

Show local state and peer list (no running daemon required):

```text
lilsync status ./tmp1
```

Create an invite:

```text
lilsync invite ./tmp1 --expire-secs 3600
```

Join from another folder (runs `watch` after a successful join):

```text
lilsync join ./tmp2 <86-char-base62-ticket>
```

Join and exit so a service manager can start the daemon:

```text
lilsync join ./tmp2 <86-char-base62-ticket> --exit
lilsync start ./tmp2
```

List known peers:

```text
lilsync peers ./tmp1
```

Remove a peer by ID:

```text
lilsync remove ./tmp1 <id>
```
