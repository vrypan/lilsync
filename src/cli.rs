//! CLI argument types (`Cli`, `Command`) and base62 ticket encoding/decoding
//! used by the invite/join flow.

use crate::identity::NodeId;
use crate::state::hex;
use clap::{Parser, Subcommand};
use std::io;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "lilsync", about = "lilsync folder sync daemon", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    pub fn daemon_log_path(&self) -> Option<std::path::PathBuf> {
        match &self.command {
            Command::Start { folder, .. } => Some(folder.join(".lil").join("sync.log")),
            _ => None,
        }
    }

    pub fn status_mode(&self) -> bool {
        matches!(
            self.command,
            Command::Watch { status: true, .. }
                | Command::Join {
                    status: true,
                    exit: false,
                    ..
                }
        )
    }
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Start the sync daemon in the background
    Start {
        /// Folder to sync
        folder: PathBuf,
        /// Human-readable name for this node
        #[arg(long)]
        name: Option<String>,
        /// Use periodic polling instead of filesystem events
        #[arg(long)]
        poll: bool,
        /// Debounce delay for filesystem events, or scan interval with --poll
        #[arg(long, value_name = "MILLIS", default_value = "500")]
        interval_ms: u64,
        /// How often to publish local root state
        #[arg(long, value_name = "SECONDS", default_value = "10")]
        announce_interval_secs: u64,
        /// RPC listen port (persisted; defaults to the last used port)
        #[arg(long, value_name = "PORT")]
        port: Option<u16>,
    },
    /// Run the sync daemon in the foreground
    Watch {
        /// Folder to sync
        folder: PathBuf,
        /// Human-readable name for this node
        #[arg(long)]
        name: Option<String>,
        /// Use periodic polling instead of filesystem events
        #[arg(long)]
        poll: bool,
        /// Debounce delay for filesystem events, or scan interval with --poll
        #[arg(long, value_name = "MILLIS", default_value = "500")]
        interval_ms: u64,
        /// How often to publish local root state
        #[arg(long, value_name = "SECONDS", default_value = "10")]
        announce_interval_secs: u64,
        /// Show a quiet peer status view instead of regular info logs
        #[arg(long)]
        status: bool,
        /// RPC listen port (persisted; defaults to the last used port)
        #[arg(long, value_name = "PORT")]
        port: Option<u16>,
    },
    /// Create a one-time join ticket and exit
    Invite {
        /// Folder whose group to invite into
        folder: PathBuf,
        /// Ticket lifetime in seconds
        #[arg(long, value_name = "SECONDS", default_value = "3600")]
        expire_secs: u64,
        /// Endpoint to embed in the ticket instead of the detected local
        /// addresses (repeatable)
        #[arg(long, value_name = "HOST:PORT")]
        endpoint: Vec<String>,
    },
    /// Join a group using a ticket
    Join {
        /// Folder to sync
        folder: PathBuf,
        /// 86-character base62 ticket from `lilsync invite`
        ticket: String,
        /// Human-readable name for this node
        #[arg(long)]
        name: Option<String>,
        /// Exit after joining instead of starting the sync daemon
        #[arg(long)]
        exit: bool,
        /// Show a quiet peer status view after joining
        #[arg(long)]
        status: bool,
        /// RPC listen port (persisted; defaults to the last used port)
        #[arg(long, value_name = "PORT")]
        port: Option<u16>,
    },
    /// Remove a peer by node ID or name
    Remove {
        /// Folder whose group to modify
        folder: PathBuf,
        /// Node ID or name to remove
        target: String,
    },
    /// List known peers
    Peers {
        /// Folder whose group to inspect
        folder: PathBuf,
    },
    /// Show local sync state and peer list
    Status {
        /// Folder to inspect
        folder: PathBuf,
    },
    /// Stop a running daemon
    Stop {
        /// Folder whose daemon to stop
        folder: PathBuf,
    },
    /// Dump stored sync state entries as JSON lines
    DumpState {
        /// Folder whose .lil state to inspect
        folder: PathBuf,
        /// Only include this path or descendants
        #[arg(long)]
        prefix: Option<String>,
    },
}

#[derive(Debug)]
pub struct JoinTicket {
    pub issuer: NodeId,
    pub secret: String,
    /// Candidate `host:port` endpoints where the issuer is reachable.
    pub endpoints: Vec<String>,
}

/// Ticket wire format, base62-encoded as one big-endian integer:
/// `[version=2][issuer 32 bytes][secret 32 bytes][endpoints utf8]` where
/// endpoints are comma-joined `host:port` strings. The leading version byte
/// is non-zero, so the exact byte length survives the base62 round trip.
const TICKET_VERSION: u8 = 2;
const TICKET_MIN_BYTES: usize = 1 + 32 + 32;
const TICKET_MAX_BYTES: usize = TICKET_MIN_BYTES + MAX_TICKET_ENDPOINT_BYTES;
pub const MAX_TICKET_ENDPOINT_BYTES: usize = 384;

pub fn parse_join_ticket(value: &str) -> io::Result<JoinTicket> {
    let bytes = ticket_base62_decode(value)?;
    if bytes.len() < TICKET_MIN_BYTES {
        return Err(io::Error::other("invalid ticket: too short"));
    }
    if bytes[0] != TICKET_VERSION {
        return Err(io::Error::other(format!(
            "unsupported ticket version {} (this build expects {TICKET_VERSION}; \
             tickets from older lilsync versions are not compatible)",
            bytes[0]
        )));
    }
    let issuer = NodeId::from_bytes(bytes[1..33].try_into().unwrap());
    let secret_arr: [u8; 32] = bytes[33..65].try_into().unwrap();
    let secret = hex(secret_arr);
    let endpoints_raw = std::str::from_utf8(&bytes[65..])
        .map_err(|_| io::Error::other("invalid ticket: endpoint list is not utf8"))?;
    let endpoints = endpoints_raw
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    Ok(JoinTicket {
        issuer,
        secret,
        endpoints,
    })
}

pub fn encode_ticket(
    node_id: NodeId,
    secret_bytes: &[u8; 32],
    endpoints: &[String],
) -> io::Result<String> {
    let joined = endpoints.join(",");
    if joined.len() > MAX_TICKET_ENDPOINT_BYTES {
        return Err(io::Error::other(format!(
            "endpoint list too long for ticket ({} bytes, max {MAX_TICKET_ENDPOINT_BYTES})",
            joined.len()
        )));
    }
    let mut combined = Vec::with_capacity(TICKET_MIN_BYTES + joined.len());
    combined.push(TICKET_VERSION);
    combined.extend_from_slice(node_id.as_bytes());
    combined.extend_from_slice(secret_bytes);
    combined.extend_from_slice(joined.as_bytes());
    Ok(ticket_base62_encode(&combined))
}

const BASE62_ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
// ceil(bytes * 8 / log2(62)) with headroom.
const TICKET_MAX_CHARS: usize = TICKET_MAX_BYTES * 8 / 5;

fn ticket_base62_encode(bytes: &[u8]) -> String {
    let mut n = bytes.to_vec();
    let mut digits = Vec::new();
    loop {
        let rem = b62_divmod(&mut n);
        digits.push(BASE62_ALPHABET[rem as usize]);
        if n.iter().all(|&b| b == 0) {
            break;
        }
    }
    digits.reverse();
    String::from_utf8(digits).unwrap()
}

fn ticket_base62_decode(s: &str) -> io::Result<Vec<u8>> {
    if s.is_empty() || s.len() > TICKET_MAX_CHARS {
        return Err(io::Error::other(format!(
            "invalid ticket: unexpected length {}",
            s.len()
        )));
    }
    let mut result = Vec::new();
    for &ch in s.as_bytes() {
        let digit = b62_char_to_digit(ch)?;
        b62_mul_add(&mut result, digit);
    }
    Ok(result)
}

fn b62_divmod(bytes: &mut [u8]) -> u8 {
    let mut rem = 0u32;
    for b in bytes.iter_mut() {
        let val = rem * 256 + *b as u32;
        *b = (val / 62) as u8;
        rem = val % 62;
    }
    rem as u8
}

/// Multiply the big-endian integer in `bytes` by 62 and add `digit`,
/// growing the buffer when the value overflows its current width.
fn b62_mul_add(bytes: &mut Vec<u8>, digit: u8) {
    let mut carry = digit as u32;
    for b in bytes.iter_mut().rev() {
        let val = *b as u32 * 62 + carry;
        *b = (val & 0xFF) as u8;
        carry = val >> 8;
    }
    while carry > 0 {
        bytes.insert(0, (carry & 0xFF) as u8);
        carry >>= 8;
    }
}

fn b62_char_to_digit(ch: u8) -> io::Result<u8> {
    match ch {
        b'0'..=b'9' => Ok(ch - b'0'),
        b'A'..=b'Z' => Ok(ch - b'A' + 10),
        b'a'..=b'z' => Ok(ch - b'a' + 36),
        _ => Err(io::Error::other(format!(
            "invalid ticket char: {}",
            ch as char
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_ticket_roundtrips() {
        let issuer = NodeId::from_bytes([7; 32]);
        let secret_bytes = [0xABu8; 32];
        let endpoints = vec!["100.64.1.2:7420".to_string(), "[fd7a::1]:7420".to_string()];
        let ticket_str = encode_ticket(issuer, &secret_bytes, &endpoints).unwrap();
        let parsed = parse_join_ticket(&ticket_str).unwrap();
        assert_eq!(parsed.issuer, issuer);
        assert_eq!(parsed.secret, hex(secret_bytes));
        assert_eq!(parsed.endpoints, endpoints);
    }

    #[test]
    fn join_ticket_roundtrips_with_leading_zero_node_id() {
        let issuer = NodeId::from_bytes([0; 32]);
        let secret_bytes = [0u8; 32];
        let ticket_str = encode_ticket(issuer, &secret_bytes, &[]).unwrap();
        let parsed = parse_join_ticket(&ticket_str).unwrap();
        assert_eq!(parsed.issuer, issuer);
        assert!(parsed.endpoints.is_empty());
    }

    #[test]
    fn rejects_old_ticket_version() {
        // A version-1 style ticket (no version byte) decodes to bytes whose
        // first byte is almost never 2; craft one explicitly.
        let mut bytes = vec![1u8];
        bytes.extend_from_slice(&[9; 64]);
        let ticket_str = ticket_base62_encode(&bytes);
        let err = parse_join_ticket(&ticket_str).unwrap_err();
        assert!(err.to_string().contains("unsupported ticket version"));
    }

    #[test]
    fn rejects_oversized_endpoint_list() {
        let issuer = NodeId::from_bytes([7; 32]);
        let long = vec!["a".repeat(MAX_TICKET_ENDPOINT_BYTES + 1)];
        assert!(encode_ticket(issuer, &[0; 32], &long).is_err());
    }
}
