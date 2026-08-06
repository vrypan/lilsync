# lil Design

## Goal

`lil` syncs a folder between a small, trusted group of nodes on a network
where they can reach each other directly — a LAN, a Tailscale tailnet, or a
VPN. Every node keeps a full copy of the folder and a full membership ledger.

There is no central node, relay server, NAT traversal, or discovery service.
Peers are bootstrapped from join tickets and thereafter exchange addresses
over their own encrypted connections. Data transfer and announcements use
direct Noise-encrypted TCP RPCs.

## Model

`peers.json` is the source of truth for group membership. A node is trusted only
if it appears as active in the local membership ledger.

- the persisted address book (`endpoints.json`) maps node IDs to candidate
  `host:port` endpoints; it is routing information, never trust.
- direct RPCs are the source of truth for sync data.
- announcements are a hint and repair trigger, not an authoritative write.
- every active node fans announcements out to every other active node it has
  an address for.

## Non-Goals

- NAT traversal or relaying; peers must be directly routable.
- Trust announcement payloads as authoritative writes.
- Implement discovery beyond ticket bootstrap plus endpoint gossip.

## Identity And Transport

Each node stores a 32-byte Ed25519 private key in `.lil/private.key`. The
Ed25519 public key is the node ID.

Peers establish a Noise `NN` session over TCP and then exchange signed identity
payloads inside the encrypted session. The Ed25519 signature binds each node ID
to the Noise handshake transcript, so the encrypted connection is associated
with the same identity used by the membership ledger.

## Addressing

Each daemon listens on a stable TCP port: set with `--port` or picked freely
on first run, then persisted in `.lil/port` and reused so previously shared
endpoints stay valid across restarts.

Peers learn each other's addresses in two ways, both over TCP:

- **Ticket bootstrap**: a join ticket embeds the issuer's `host:port`
  endpoints, so the joiner can connect with no prior state.
- **Endpoint gossip**: each daemon periodically announces its listen port and
  interface addresses to active members via an `Endpoints` message. The
  receiver stores the advertised endpoints and puts the connection's observed
  source address (paired with the advertised port) first, since that address
  is known to be routable from here.

Learned endpoints are persisted in `.lil/endpoints.json`. When connecting,
a node tries each candidate endpoint in order; the Noise handshake pins the
expected node identity, so a stale or wrong address fails closed. Endpoints
may be DNS names; they are resolved at dial time.

If a peer has no known working address, RPCs to it time out instead of
falling back to a relay. If every member changes all of its addresses while
the group is offline, the group must re-bootstrap with a fresh invite.

## Sync Tree

The sync tree is a path-based directory-mirroring Merkle tree. It provides:

- **A single root hash** describing the complete folder state.
- **Locality**: a change to `src/foo/bar.rs` recomputes only the file leaf, the
  `src/foo/` node, the `src/` node, and the root.

### File Leaves

```
leaf_hash = blake3(content_hash ++ lamport_le64 ++ changed_at_ms_le64 ++ origin_bytes)
```

- `content_hash`: BLAKE3 of file content; `TOMBSTONE_HASH` for deletions.
- `lamport_le64`: 8-byte little-endian Lamport clock.
- `changed_at_ms_le64`: 8-byte little-endian wall-clock timestamp.
- `origin_bytes`: UTF-8 node ID of the originator.

Deleted files remain as tombstone leaves until garbage collection can prove the
active peers have converged on the deletion.

### Directory Nodes

```
dir_hash = blake3("name1\0" ++ hash1 ++ "name2\0" ++ hash2 ++ ...)
```

Children are sorted lexicographically by name.

## RPCs

All RPCs run over a fresh Noise-encrypted TCP connection.

| RPC | Purpose |
|---|---|
| `Join` | Consume an invite token and return the full member ledger |
| `GetRoot` | Read a peer's state root, live root, and Lamport clock |
| `GetNode` | Read one Merkle tree node by path prefix |
| `GetEntry` | Read one replicated entry by path |
| `GetObject` | Stream file content by content hash |
| `Announce` | Deliver a sync-state, filesystem-changed, peers, or endpoints announcement |

File content is streamed in encrypted chunks. The receiver verifies the BLAKE3
hash before installing a downloaded object.

## Announcements

Announcements are sent by direct fanout RPC to active peers. They carry an
`origin` node ID and are accepted only if the origin is active in the local
member ledger and matches the authenticated RPC peer.

| Message | Published by | When |
|---|---|---|
| `FilesystemChanged` | any node | after local filesystem changes |
| `SyncState` | any node | on startup and periodically |
| `Peers` | any node | after join/removal and periodically |
| `Endpoints` | any node | on startup, after a join, and periodically |

Receivers use announcements as hints:

- a different root schedules a Merkle reconciliation against the origin.
- a filesystem hint can reduce the tree walk to changed prefixes.
- a peers announcement merges newer member ledger entries.
- an endpoints announcement updates the persisted address book.

## Membership

### Invite Ticket

An existing member creates an invite token and stores it in `.lil/invites.json`.
The ticket encodes a version byte, the issuer node ID, the token secret, and
the issuer's `host:port` endpoints (detected from local interfaces, or set
with `--endpoint`). Tokens are single-use and expire.

### Join Flow

1. Joiner seeds its address book with the issuer endpoints from the ticket.
2. Joiner connects to the issuer and sends `Join`.
3. Issuer consumes the invite token.
4. Issuer adds the joiner to its member ledger.
5. Issuer returns the full member ledger.
6. Joiner writes that ledger to `.lil/peers.json` and persists the issuer's
   endpoints to `.lil/endpoints.json`.
7. If not started with `--exit`, the joiner starts its sync daemon and
   announces its own endpoints to the group, which is how the issuer (and
   everyone else) learns the joiner's address.

### Member Removal

Any node can mark another member as removed. Removed entries remain in the
ledger with their Lamport values so stale announcements cannot resurrect old
members. Remaining peers apply the removal when they receive a newer `Peers`
announcement or reconnect and reconcile.

## Tombstone Garbage Collection

Deletes are retained as tombstones until active peers have reported matching
roots for the deletion. Each node also persists a GC watermark in
`.lil/gc-watermark.bin` so old versions cannot be reintroduced after restart.

When active peers converge on the same root, tombstones covered by the
converged watermark can be pruned and the new state is announced.

## State Files

All state lives in `<sync-folder>/.lil/`:

| File | Contents |
|---|---|
| `private.key` | 32-byte Ed25519 private key |
| `port` | persisted RPC listen port |
| `endpoints.json` | last known peer endpoints |
| `daemon.lock` | exclusive sync process lock |
| `peers.json` | full membership ledger |
| `invites.json` | outstanding invite tokens with expiry |
| `entries.bin` | persisted replicated entry index |
| `lamport` | persisted Lamport clock |
| `gc-watermark.bin` | persisted tombstone GC watermark |

### `peers.json` Schema

```json
{
  "members": [
    { "id": "<node-id>", "status": "active", "lamport": 42, "name": "macbook" },
    { "id": "<node-id>", "status": "removed", "lamport": 105, "name": null }
  ]
}
```

Older files may contain a `topic_id` field from the previous transport. It is
ignored and omitted on the next save.

## Risks

### Direct-Reachability Availability

Peers cannot sync unless they can open TCP connections to each other's
persisted or gossiped addresses. This is an intentional trade-off after
removing relay and NAT traversal support. A group whose members all change
every address while fully offline cannot heal itself and needs a fresh
invite; a single still-valid address anywhere in the group is enough to
re-propagate current endpoints to everyone.

### Authorization Drift

Member lists are persisted independently and may diverge while a node is
offline. Periodic `Peers` announcements repair this when nodes are online
together.

### Announcement Fanout

Every announcement is sent directly to every active peer. This is simple and
works for small groups, but large groups or high write volume can produce many
short TCP connections.

### Backward Compatibility

Existing data and private keys are reused, but peer IDs are now stored as raw
Ed25519 public-key hex strings. Very old `peers.json` files that only contain
transport-specific node ID encodings may need to be rejoined.
