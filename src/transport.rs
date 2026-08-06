//! Noise NN handshake with an Ed25519 authentication payload exchanged after
//! the handshake. Provides `NoiseConnection` for length-prefixed encrypted
//! frame I/O over any async byte stream.

use crate::identity::{Identity, NodeId};
use serde::{Deserialize, Serialize};
use snow::TransportState;
use std::future::Future;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

const NOISE_PATTERN: &str = "Noise_NN_25519_ChaChaPoly_BLAKE2s";
const MAX_HANDSHAKE_BYTES: usize = 4096;
const MAX_BLOB_BYTES: usize = 16 * 1024 * 1024;
const MAX_PLAINTEXT_CHUNK: usize = 16 * 1024;
const MAX_CIPHERTEXT_CHUNK: usize = MAX_PLAINTEXT_CHUNK + 16;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize, Deserialize)]
struct AuthPayload {
    node_id: String,
    signature: Vec<u8>,
}

pub struct NoiseConnection {
    stream: TcpStream,
    noise: TransportState,
    peer: NodeId,
    read_buf: Vec<u8>,
}

impl NoiseConnection {
    /// Connect to a `host:port` endpoint; hostnames are resolved via DNS.
    pub async fn connect(endpoint: &str, identity: &Identity) -> io::Result<Self> {
        let stream = TcpStream::connect(endpoint).await?;
        let mut noise = snow::Builder::new(
            NOISE_PATTERN
                .parse()
                .map_err(|err| io::Error::other(format!("invalid noise pattern: {err}")))?,
        )
        .build_initiator()
        .map_err(|err| io::Error::other(format!("build noise initiator: {err}")))?;

        let mut stream = stream;
        let mut out = vec![0_u8; MAX_HANDSHAKE_BYTES];
        let len = noise
            .write_message(&[], &mut out)
            .map_err(|err| io::Error::other(format!("noise write handshake: {err}")))?;
        write_plain_frame(&mut stream, &out[..len]).await?;

        let msg =
            read_plain_frame_timeout(&mut stream, MAX_HANDSHAKE_BYTES, HANDSHAKE_TIMEOUT).await?;
        let mut inbound = vec![0_u8; MAX_HANDSHAKE_BYTES];
        noise
            .read_message(&msg, &mut inbound)
            .map_err(|err| io::Error::other(format!("noise read handshake: {err}")))?;

        let handshake_hash = noise.get_handshake_hash().to_vec();
        let transport = noise
            .into_transport_mode()
            .map_err(|err| io::Error::other(format!("noise transport mode: {err}")))?;
        let mut connection = Self {
            stream,
            noise: transport,
            peer: NodeId::from_bytes([0; 32]),
            read_buf: Vec::new(),
        };
        connection.send_auth(identity, &handshake_hash).await?;
        let peer = connection.recv_auth(&handshake_hash).await?;
        connection.peer = peer;
        Ok(connection)
    }

    pub async fn accept(mut stream: TcpStream, identity: &Identity) -> io::Result<Self> {
        let mut noise = snow::Builder::new(
            NOISE_PATTERN
                .parse()
                .map_err(|err| io::Error::other(format!("invalid noise pattern: {err}")))?,
        )
        .build_responder()
        .map_err(|err| io::Error::other(format!("build noise responder: {err}")))?;

        let msg =
            read_plain_frame_timeout(&mut stream, MAX_HANDSHAKE_BYTES, HANDSHAKE_TIMEOUT).await?;
        let mut inbound = vec![0_u8; MAX_HANDSHAKE_BYTES];
        noise
            .read_message(&msg, &mut inbound)
            .map_err(|err| io::Error::other(format!("noise read handshake: {err}")))?;

        let mut out = vec![0_u8; MAX_HANDSHAKE_BYTES];
        let len = noise
            .write_message(&[], &mut out)
            .map_err(|err| io::Error::other(format!("noise write handshake: {err}")))?;
        write_plain_frame(&mut stream, &out[..len]).await?;

        let handshake_hash = noise.get_handshake_hash().to_vec();
        let transport = noise
            .into_transport_mode()
            .map_err(|err| io::Error::other(format!("noise transport mode: {err}")))?;
        let mut connection = Self {
            stream,
            noise: transport,
            peer: NodeId::from_bytes([0; 32]),
            read_buf: Vec::new(),
        };
        let peer = connection.recv_auth(&handshake_hash).await?;
        connection.send_auth(identity, &handshake_hash).await?;
        connection.peer = peer;
        Ok(connection)
    }

    pub fn peer(&self) -> NodeId {
        self.peer
    }

    pub async fn send_json<T: Serialize>(&mut self, value: &T) -> io::Result<()> {
        let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
        self.send_blob(&bytes).await
    }

    pub async fn recv_json<T: for<'de> Deserialize<'de>>(&mut self) -> io::Result<T> {
        let bytes = self.recv_blob(MAX_BLOB_BYTES).await?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }

    pub async fn send_plain_chunk(&mut self, chunk: &[u8]) -> io::Result<()> {
        if chunk.len() > MAX_PLAINTEXT_CHUNK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("plaintext chunk too large: {}", chunk.len()),
            ));
        }
        let mut encrypted = vec![0_u8; chunk.len() + 16];
        let len = self
            .noise
            .write_message(chunk, &mut encrypted)
            .map_err(|err| io::Error::other(format!("noise encrypt: {err}")))?;
        write_plain_frame(&mut self.stream, &encrypted[..len]).await
    }

    pub async fn recv_plain_chunk(&mut self) -> io::Result<Vec<u8>> {
        let encrypted = read_plain_frame(&mut self.stream, MAX_CIPHERTEXT_CHUNK).await?;
        let mut plain = vec![0_u8; encrypted.len()];
        let len = self
            .noise
            .read_message(&encrypted, &mut plain)
            .map_err(|err| io::Error::other(format!("noise decrypt: {err}")))?;
        plain.truncate(len);
        Ok(plain)
    }

    pub async fn recv_plain_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let mut filled = 0usize;
        while filled < buf.len() {
            if self.read_buf.is_empty() {
                self.read_buf = self.recv_plain_chunk().await?;
            }
            let take = (buf.len() - filled).min(self.read_buf.len());
            buf[filled..filled + take].copy_from_slice(&self.read_buf[..take]);
            self.read_buf.drain(..take);
            filled += take;
        }
        Ok(())
    }

    async fn send_blob(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() > MAX_BLOB_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("blob too large: {}", bytes.len()),
            ));
        }
        self.send_plain_chunk(&(bytes.len() as u64).to_be_bytes())
            .await?;
        for chunk in bytes.chunks(MAX_PLAINTEXT_CHUNK) {
            self.send_plain_chunk(chunk).await?;
        }
        Ok(())
    }

    async fn recv_blob(&mut self, max_len: usize) -> io::Result<Vec<u8>> {
        let mut len_buf = [0_u8; 8];
        self.recv_plain_exact(&mut len_buf).await?;
        let len = u64::from_be_bytes(len_buf) as usize;
        if len > max_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("blob length {len} exceeds maximum {max_len}"),
            ));
        }
        let mut bytes = vec![0_u8; len];
        self.recv_plain_exact(&mut bytes).await?;
        Ok(bytes)
    }

    async fn send_auth(&mut self, identity: &Identity, handshake_hash: &[u8]) -> io::Result<()> {
        let message = auth_message(identity.node_id(), handshake_hash);
        let payload = AuthPayload {
            node_id: identity.node_id().to_string(),
            signature: identity.sign(&message).to_vec(),
        };
        self.send_json(&payload).await
    }

    async fn recv_auth(&mut self, handshake_hash: &[u8]) -> io::Result<NodeId> {
        let payload: AuthPayload = self.recv_json().await?;
        let node_id: NodeId = payload.node_id.parse()?;
        let signature: [u8; 64] = payload
            .signature
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid signature length"))?;
        node_id.verify(&auth_message(node_id, handshake_hash), &signature)?;
        Ok(node_id)
    }
}

fn auth_message(node_id: NodeId, handshake_hash: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(32 + handshake_hash.len() + 24);
    message.extend_from_slice(b"lilsync-noise-auth-v1");
    message.extend_from_slice(node_id.as_bytes());
    message.extend_from_slice(handshake_hash);
    message
}

async fn write_plain_frame(stream: &mut TcpStream, bytes: &[u8]) -> io::Result<()> {
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(bytes).await
}

async fn read_plain_frame(stream: &mut TcpStream, max_len: usize) -> io::Result<Vec<u8>> {
    let len = stream.read_u32().await? as usize;
    if len > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {len} exceeds maximum {max_len}"),
        ));
    }
    let mut bytes = vec![0_u8; len];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}

async fn read_plain_frame_timeout(
    stream: &mut TcpStream,
    max_len: usize,
    duration: Duration,
) -> io::Result<Vec<u8>> {
    timeout_io(duration, read_plain_frame(stream, max_len)).await
}

pub async fn timeout_io<T>(
    duration: Duration,
    future: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    timeout(duration, future)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "operation timed out"))?
}

pub fn max_plaintext_chunk() -> usize {
    MAX_PLAINTEXT_CHUNK
}
