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
    },
    /// Create a one-time join ticket and exit
    Invite {
        /// Folder whose group to invite into
        folder: PathBuf,
        /// Ticket lifetime in seconds
        #[arg(long, value_name = "SECONDS", default_value = "3600")]
        expire_secs: u64,
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
    /// Discover lilsync nodes advertised on the local network via mDNS
    Scan {
        /// Stay alive and refresh the list as peers appear or disappear
        #[arg(long)]
        watch: bool,
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

pub struct JoinTicket {
    pub issuer: NodeId,
    pub secret: String,
}

pub fn parse_join_ticket(value: &str) -> io::Result<JoinTicket> {
    let bytes = ticket_base62_decode(value)?;
    let issuer = NodeId::from_bytes(bytes[..32].try_into().unwrap());
    let secret_arr: [u8; 32] = bytes[32..].try_into().unwrap();
    let secret = hex(secret_arr);
    Ok(JoinTicket { issuer, secret })
}

pub fn encode_ticket(node_id: NodeId, secret_bytes: &[u8; 32]) -> String {
    let mut combined = [0u8; 64];
    combined[..32].copy_from_slice(node_id.as_bytes());
    combined[32..].copy_from_slice(secret_bytes);
    ticket_base62_encode(&combined)
}

const BASE62_ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
pub const TICKET_CHARS: usize = 86;

fn ticket_base62_encode(bytes: &[u8; 64]) -> String {
    let mut n = *bytes;
    let mut digits = Vec::with_capacity(TICKET_CHARS);
    loop {
        let rem = b62_divmod(&mut n);
        digits.push(BASE62_ALPHABET[rem as usize]);
        if n.iter().all(|&b| b == 0) {
            break;
        }
    }
    while digits.len() < TICKET_CHARS {
        digits.push(BASE62_ALPHABET[0]);
    }
    digits.reverse();
    String::from_utf8(digits).unwrap()
}

fn ticket_base62_decode(s: &str) -> io::Result<[u8; 64]> {
    if s.len() != TICKET_CHARS {
        return Err(io::Error::other(format!(
            "invalid ticket: expected {} chars, got {}",
            TICKET_CHARS,
            s.len()
        )));
    }
    let mut result = [0u8; 64];
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

fn b62_mul_add(bytes: &mut [u8], digit: u8) {
    let mut carry = digit as u32;
    for b in bytes.iter_mut().rev() {
        let val = *b as u32 * 62 + carry;
        *b = (val & 0xFF) as u8;
        carry = val >> 8;
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
        let ticket_str = encode_ticket(issuer, &secret_bytes);
        assert_eq!(ticket_str.len(), TICKET_CHARS);
        let parsed = parse_join_ticket(&ticket_str).unwrap();
        assert_eq!(parsed.issuer, issuer);
        assert_eq!(parsed.secret, hex(secret_bytes));
    }
}
