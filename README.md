> [!WARNING]
> `lilsync` is **experimental**! There may be bugs that could DELETE or EXPOSE
> your files. I use it, but **use it at your own risk.**

# lil — a little tool to sync your files

`lilsync` syncs a folder between a small, **trusted** group of nodes.

- Designed for peer-to-peer sync between your own machines or other trusted
  peers.

- End-to-end encrypted: peers talk over Noise-encrypted TCP connections
  authenticated by their Ed25519 node keys.

- Works on any network where your nodes can reach each other directly — a
  LAN, a [Tailscale](https://tailscale.com) tailnet, or a VPN. Peers are
  bootstrapped from join tickets (which embed the inviter's addresses) and
  keep each other's addresses up to date over the encrypted connections.
  There is no relay, NAT traversal, or central discovery service.

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
| `port` | Persisted RPC listen port (reused across restarts) |
| `endpoints.json` | Last known peer addresses |
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

This prints a base62 ticket that embeds the inviter's node ID, a one-time
secret, and the addresses where the inviter is reachable (detected from its
local interfaces — including Tailscale/VPN addresses). Use
`--endpoint <host:port>` to override the detected addresses, e.g. with a DNS
name. On the second node:

```bash
lilsync join /path/to/folder2 <ticket>
```

`join` completes the handshake and then starts the daemon in the foreground.
Use `--exit` to join and exit instead, then run `lilsync start` to launch the
background daemon.

## Subcommands

```
lilsync start  <folder> [--name <name>] [--poll] [--interval-ms <ms>]
                    [--announce-interval-secs <secs>] [--port <port>]
lilsync watch  <folder> [--name <name>] [--poll] [--interval-ms <ms>]
                    [--announce-interval-secs <secs>] [--status] [--port <port>]
lilsync stop   <folder>
lilsync status <folder>
lilsync invite <folder> [--expire-secs <secs>] [--endpoint <host:port>]...
lilsync join   <folder> <ticket> [--name <name>] [--exit] [--port <port>]
lilsync peers  <folder>
lilsync remove <folder> <id>
```

- `start` forks into the background; logs go to `<folder>/.lil/sync.log`.
- `watch` runs in the foreground; logs go to the terminal.
- `--status` (watch only) shows a live colour peer-status view instead of log lines.
- `--name` sets a human-readable label shared with peers while the daemon runs.
- `--poll` uses filesystem polling instead of native OS notifications.
- `--interval-ms` sets the watcher debounce window (default 500 ms).
- `--announce-interval-secs` sets how often `SyncState` is broadcast (default 10 s).
- `--port` sets the RPC listen port. The port is persisted in `.lil/port` and
  reused on later runs, so peers keep a stable address for this node; without
  `--port` the first run picks a free port. Each synced folder on a machine
  needs its own port.
- `--expire-secs` sets invite lifetime (default 3600 s).
- `--endpoint` overrides the addresses embedded in the ticket (repeatable);
  useful when the joining node should connect via a specific address or DNS
  name.
- `--exit` makes `join` stop after writing group state instead of starting the daemon.

## Networking

Peers find each other in two steps, both over plain TCP:

- **Bootstrap**: a join ticket carries the inviter's addresses, so the
  joining node knows where to connect with no discovery protocol involved.
- **Refresh**: every node periodically gossips its listen port and interface
  addresses to the other members over the encrypted RPC channel, and each
  receiver also records the address it actually saw the sender connect from.
  Last known addresses are persisted in `.lil/endpoints.json` and reused
  across restarts.

This means lilsync works unchanged across a LAN, a Tailscale tailnet, or any
routed network — no multicast/mDNS required. If every peer changes all of its
addresses while the group is fully offline, they can no longer find each
other; issue a fresh invite from one node to re-bootstrap.

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

## Running as a systemd Service

See [deploy/systemd/README.md](deploy/systemd/README.md) for running
`lilsync` under systemd, including named instances for syncing multiple
folders.

## Debugging

Use `lilsync watch` to keep the daemon in the foreground. Log verbosity is
controlled via `RUST_LOG` (default: `info`):

```bash
RUST_LOG=info  lilsync watch /tmp/node-a
RUST_LOG=debug lilsync watch /tmp/node-a
```

When running as a background daemon (`lilsync start`), logs are written to
`<folder>/.lil/sync.log`. `RUST_LOG` is still respected.
