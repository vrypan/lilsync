> [!WARNING]
> `lilsync` is **experimental**! There may be bugs that could DELETE or EXPOSE
> your files. I use it, but **use it at your own risk.**

# lil — a little tool to sync your files

`lilsync` syncs a folder between a small, **trusted** group of nodes.

- Designed for peer-to-peer sync between your own machines or other trusted
  peers.

- End-to-end encrypted on the LAN: peers discover each other with mDNS and use
  Noise-encrypted TCP connections authenticated by their Ed25519 node keys.

- LAN-only by design: there is no relay, NAT traversal, or remote discovery.

- No central node: every node is equal. Each keeps a full copy of the synced
  folder and each can invite new nodes.

> [!NOTE]
> See [SECURITY.md](SECURITY.md) for the full threat model and known limitations.

## Installation

### Release binaries

Download the archive for your platform from the
[latest release](https://github.com/vrypan/lil/releases/latest), then install the
`lilsync` binary somewhere on your `PATH`:

```bash
tar -xzf lilsync-*.tar.gz
sudo install -m 0755 lilsync-*/lilsync /usr/local/bin/lilsync
```

Release archives are named by version and target, for example
`lilsync-0.3.2-aarch64-apple-darwin.tar.gz` or
`lilsync-0.3.2-x86_64-unknown-linux-musl.tar.gz`.

### Homebrew

On macOS, install with Homebrew:

```bash
brew install vrypan/tap/lilsync
```

### Build from source

Install Rust 1.94.1 or newer, then build the release binary:

```bash
cargo build --release
```

The binary is `target/release/lilsync`. To install it:

```bash
sudo install -m 0755 target/release/lilsync /usr/local/bin/lilsync
```

## Basic Usage

Start the sync daemon in the background (creates a new single-node group on
first run):

```bash
lilsync start /path/to/folder
```

Logs are written to `/path/to/folder/.lil/sync.log` (10 MB rolling, one
backup). To stop the daemon:

```bash
lilsync stop /path/to/folder
```

To run in the foreground instead (useful for debugging):

```bash
lilsync watch /path/to/folder
```

State is stored inside the synced folder under `.lil/`:

| File | Purpose |
|---|---|
| `private.key` | Node identity key |
| `private.key` | Node identity key |
| `daemon.lock` | Exclusive process lock |
| `daemon.pid` | PID of the running daemon (written by `start`, removed by `stop`) |
| `sync.log` | Daemon log file (rolling, 10 MB cap) |
| `peers.json` | Known peers and group membership |
| `invites.json` | Pending invite tokens |
| `entries.bin` | Persisted entry index (survives restarts) |
| `lamport` | Persisted Lamport clock |
| `gc-watermark.bin` | Persisted tombstone garbage-collection watermark |

## Adding a Second Node

Generate an invite on the first node:

```bash
lilsync invite /path/to/folder
```

This prints an 86-character base62 ticket. On the second node:

```bash
lilsync join /path/to/folder2 <ticket>
```

`join` completes the handshake and then starts the daemon in the foreground.
Use `--exit` to join and exit instead, then run `lilsync start` to launch the
background daemon.

## Subcommands

```
lilsync start  <folder> [--name <name>] [--poll] [--interval-ms <ms>]
                    [--announce-interval-secs <secs>]
lilsync watch  <folder> [--name <name>] [--poll] [--interval-ms <ms>]
                    [--announce-interval-secs <secs>] [--status]
lilsync stop   <folder>
lilsync status <folder>
lilsync invite <folder> [--expire-secs <secs>]
lilsync join   <folder> <ticket> [--name <name>] [--exit]
lilsync peers  <folder>
lilsync remove <folder> <id>
```

- `start` forks into the background; logs go to `<folder>/.lil/sync.log`.
- `watch` runs in the foreground; logs go to the terminal.
- `--status` (watch only) shows a live colour peer-status view instead of log lines.
- `--name` sets a human-readable label advertised via mDNS while the daemon runs.
- `--poll` uses filesystem polling instead of native OS notifications.
- `--interval-ms` sets the watcher debounce window (default 500 ms).
- `--announce-interval-secs` sets how often `SyncState` is broadcast (default 10 s).
- `--expire-secs` sets invite lifetime (default 3600 s).
- `--exit` makes `join` stop after writing group state instead of starting the daemon.

## Ignore Rules

Create `.nolil` inside the synced folder to exclude paths:

```text
files/
*.tmp
build/
!build/keep.txt
```

Supported syntax (gitignore-like):

- blank lines and `#` comments are ignored
- `!` negates a rule
- `*`, `?`, `**` wildcards
- leading `/` anchors to the root
- trailing `/` matches directories only

When `.nolil` changes, `lilsync` rescans the folder. Newly ignored paths stop
being tracked locally; they are **not** deleted on remote peers.

## Notes

- `.lil/` is always excluded from sync. Temporary files for in-flight
  transfers (`recv-*`) are stored there and removed on completion or error.
- File content is streamed over encrypted TCP without buffering the whole file in
  memory; BLAKE3 hash is verified before the temp file is renamed into place.
- Up to 8 file downloads run in parallel per reconciliation pass.
- Periodic `SyncState` broadcasts (default every 10 s) drive repair: any node
  with a different root hash initiates a Merkle tree sync. Filesystem-change
  announcements also include a small bounded tree hint to reduce follow-up RPCs.
- Tombstones (records of deleted files) are persisted across restarts and
  garbage-collected after all active peers report the same state root. Nodes
  publish GC watermarks so stale tombstones are not accepted again later.
- Some OS metadata files are always ignored: `.DS_Store`, `Thumbs.db`,
  `Desktop.ini`, `._*`, `.Spotlight-V100`, `$RECYCLE.BIN`, `lost+found`.
- Empty directories are not tracked. If the last file in a directory is
  deleted, the empty directory is removed on peers.

## Debugging

Use `lilsync watch` to keep the daemon in the foreground. Log verbosity is
controlled via `RUST_LOG` (default: `info`):

```bash
RUST_LOG=info  lilsync watch /tmp/node-a
RUST_LOG=debug lilsync watch /tmp/node-a
```

When running as a background daemon (`lilsync start`), logs are written to
`<folder>/.lil/sync.log`. `RUST_LOG` is still respected.
