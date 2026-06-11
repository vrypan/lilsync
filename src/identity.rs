//! Ed25519 node identity: key generation, loading from and saving to disk,
//! the `NodeId` (verifying key) type, and signature helpers.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use std::fmt;
use std::fs;
use std::io;
use std::path::Path;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodeId([u8; 32]);

impl NodeId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn verify(&self, message: &[u8], signature: &[u8; 64]) -> io::Result<()> {
        let key = VerifyingKey::from_bytes(&self.0)
            .map_err(|err| io::Error::other(format!("invalid node id: {err}")))?;
        let signature = Signature::from_bytes(signature);
        key.verify(message, &signature)
            .map_err(|err| io::Error::other(format!("invalid identity signature: {err}")))
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for NodeId {
    type Err = io::Error;

    fn from_str(value: &str) -> io::Result<Self> {
        if value.len() != 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "invalid node id length: expected 64 hex chars, got {}",
                    value.len()
                ),
            ));
        }
        let mut bytes = [0_u8; 32];
        for (i, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
            let s = std::str::from_utf8(chunk)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
            bytes[i] = u8::from_str_radix(s, 16)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        }
        Ok(Self(bytes))
    }
}

#[derive(Clone, Debug)]
pub struct Identity {
    signing_key: SigningKey,
}

impl Identity {
    pub fn load_or_create(path: &Path) -> io::Result<Self> {
        if let Ok(bytes) = fs::read(path) {
            let bytes: [u8; 32] = bytes
                .try_into()
                .map_err(|_| io::Error::other("invalid ed25519 key file length"))?;
            return Ok(Self {
                signing_key: SigningKey::from_bytes(&bytes),
            });
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut bytes = [0u8; 32];
        getrandom::getrandom(&mut bytes).map_err(io::Error::other)?;
        fs::write(path, bytes)?;
        Ok(Self {
            signing_key: SigningKey::from_bytes(&bytes),
        })
    }

    pub fn node_id(&self) -> NodeId {
        NodeId::from_bytes(self.signing_key.verifying_key().to_bytes())
    }

    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        self.signing_key.sign(message).to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_hex_roundtrips() {
        let id = NodeId::from_bytes([7; 32]);
        assert_eq!(id.to_string().parse::<NodeId>().unwrap(), id);
    }

    #[test]
    fn identity_signatures_verify() {
        let identity = Identity {
            signing_key: SigningKey::from_bytes(&[9; 32]),
        };
        let message = b"hello";
        let signature = identity.sign(message);
        identity.node_id().verify(message, &signature).unwrap();
    }
}
