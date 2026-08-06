# Security Assessment

`lil` is designed for syncing folders between a small group of trusted nodes
on a network you control — a LAN, a Tailscale tailnet, or a VPN. This document
describes the threat model and known limitations.

## Threat Model

`lil` is **not** designed to defend against a compromised group member, a
compromised machine, or a sophisticated adversary with physical access to any
node. It is designed to be safe against outsiders who have network access to
the TCP port the daemon listens on. Exposing that port on a private network
segment (LAN, tailnet, VPN) is the supported configuration; exposing it on
the public internet enlarges the denial-of-service surface described below
and is not recommended.

## Attack Surface

### File content — strong protection

An outsider cannot read your files passively.

Peers communicate over TCP encrypted with Noise. Each side proves its Ed25519
node identity inside the encrypted handshake transcript. File transfer RPCs
are gated by the membership ledger in `peers.json`; a non-member can connect,
but its requests are rejected before any file data is sent.

Peer addresses are pure routing information, never trust: whether an address
came from a ticket, gossip, or DNS, the connection is only used after the
responder proves it holds the expected node key. A wrong or maliciously
planted address can misdirect a TCP connection but cannot cause impersonation
or leak content.

The realistic paths to file content are:

- **Invite token interception**: tokens are single-use and time-limited, but
  if transmitted insecurely an attacker who can reach the issuer's port could
  use one before the intended recipient. Share invite tokens over an
  already-secure channel; note that tickets now also embed the issuer's
  addresses, so a leaked ticket also reveals where the issuer is reachable.
- **Key file theft**: stealing `.lil/private.key` from a compromised machine
  allows impersonating that node. This requires physical or OS-level access.

### File metadata — member-visible announcements

Announcements are delivered by direct fanout to active peers. Announcement
payloads include node IDs, state roots, Lamport clocks, bounded tree hints
for filesystem changes, and each node's addresses and listen port. They are
encrypted in transit, but any current group member can see them.

Removed members stop receiving new announcements once remaining peers have
applied the removal. A removed member that still has a valid old key cannot
download file content from peers that have the removal in their local member
ledger.

### Discovery metadata — none broadcast

There is no discovery protocol: nothing is broadcast or multicast on the
network. Addresses travel inside tickets (shared out of band) and inside
authenticated, encrypted connections between members. An outsider scanning
the network sees only an open TCP port that speaks a Noise handshake.

### Denial of service — lightly mitigated

The TCP listener accepts inbound connections from anywhere it is reachable,
and its port is stable across restarts (persisted in `.lil/port`), which
makes it a predictable target. Handshakes and RPCs have timeouts, and
non-member requests are rejected, but there is no global rate limit or
connection cap. A targeted flood could still exhaust file descriptors or CPU;
this is the main reason to keep the port off the public internet.

## Summary

| Threat | Risk | Status |
|---|---|---|
| Passive network sniffing | Low | Noise encryption on all connections |
| Unauthorized file download | Very low | Membership allowlist on all file RPCs |
| Invite token interception | Low–medium | Single-use, time-limited; depends on how it is shared |
| Key file theft (`private.key`) | Low | Requires physical or OS-level access |
| Address misdirection (bad ticket/gossip/DNS) | Very low | Node identity pinned in handshake; fails closed |
| Removed-member metadata access | Low | Stops after peers learn the removal |
| DoS via connection flood | Low–medium | Per-operation timeouts; no rate limit; stable port — keep it off the public internet |

## Files to Protect

| File | Secret | Consequence if leaked |
|---|---|---|
| `.lil/private.key` | Node identity key | Attacker can impersonate this node |
| `.lil/peers.json` | Member node IDs and status ledger | Attacker learns group membership |
| `.lil/endpoints.json` | Peer addresses | Attacker learns where group members are reachable |
| Invite tokens | Printed to stdout at generation time | Attacker can join the group if used before the intended recipient; token also reveals the issuer's addresses |
