//! Byte-compatible port of `@getpaseo/relay` crypto (tweetnacl `box`):
//! Curve25519 key agreement + XSalsa20-Poly1305, bundle = nonce(24) || box.

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use crypto_box::aead::{Aead, AeadCore, OsRng};
use crypto_box::{PublicKey, SalsaBox, SecretKey};
use serde::{Deserialize, Serialize};
use std::path::Path;

const NONCE_LEN: usize = 24;

pub struct DaemonIdentity {
    pub server_id: String,
    pub secret: SecretKey,
    pub public_b64: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredKeyPair {
    v: u8,
    public_key_b64: String,
    secret_key_b64: String,
}

impl DaemonIdentity {
    /// Same on-disk layout as the Node daemon (`server-id`, `daemon-keypair.json`).
    pub fn load_or_create(home: &Path) -> std::io::Result<DaemonIdentity> {
        std::fs::create_dir_all(home)?;
        let id_path = home.join("server-id");
        let server_id = match std::fs::read_to_string(&id_path) {
            Ok(s) if !s.trim().is_empty() => s.trim().to_string(),
            _ => {
                let mut raw = [0u8; 9];
                getrandom_fill(&mut raw);
                let id = format!("srv_{}", URL_SAFE_NO_PAD.encode(raw));
                crate::util::write_atomic(&id_path, format!("{id}\n").as_bytes(), true)?;
                id
            }
        };

        let key_path = home.join("daemon-keypair.json");
        if let Ok(raw) = std::fs::read_to_string(&key_path) {
            if let Ok(stored) = serde_json::from_str::<StoredKeyPair>(&raw) {
                if let Ok(bytes) = STANDARD.decode(stored.secret_key_b64.as_bytes()) {
                    if let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice()) {
                        let secret = SecretKey::from(arr);
                        let public_b64 = STANDARD.encode(secret.public_key().as_bytes());
                        return Ok(DaemonIdentity { server_id, secret, public_b64 });
                    }
                }
            }
            crate::warn!("daemon keypair unreadable, regenerating");
        }
        let secret = SecretKey::generate(&mut OsRng);
        let public_b64 = STANDARD.encode(secret.public_key().as_bytes());
        let stored = StoredKeyPair {
            v: 2,
            public_key_b64: public_b64.clone(),
            secret_key_b64: STANDARD.encode(secret.to_bytes()),
        };
        let json = serde_json::to_string_pretty(&stored).unwrap_or_default() + "\n";
        crate::util::write_atomic(&key_path, json.as_bytes(), true)?;
        Ok(DaemonIdentity { server_id, secret, public_b64 })
    }

    /// `https://app.paseo.sh/#offer=<base64url(json)>`, same as `encodeOfferToFragmentUrl`.
    pub fn pairing_url(&self, app_base_url: &str, relay_endpoint: &str, use_tls: bool) -> String {
        let offer = serde_json::json!({
            "v": 2,
            "serverId": self.server_id,
            "daemonPublicKeyB64": self.public_b64,
            "relay": { "endpoint": relay_endpoint, "useTls": use_tls },
        });
        let encoded = URL_SAFE_NO_PAD.encode(offer.to_string().as_bytes());
        format!("{}/#offer={}", app_base_url.trim_end_matches('/'), encoded)
    }
}

fn getrandom_fill(buf: &mut [u8]) {
    // uuid v4 is backed by the OS CSPRNG; avoids a direct rand_core dependency.
    let random = uuid::Uuid::new_v4();
    let n = buf.len().min(16);
    buf[..n].copy_from_slice(&random.as_bytes()[..n]);
}

pub struct Channel {
    cipher: SalsaBox,
}

impl Channel {
    pub fn new(secret: &SecretKey, peer_public_b64: &str) -> Result<Channel, String> {
        let bytes = STANDARD
            .decode(peer_public_b64.trim().as_bytes())
            .map_err(|_| "Invalid public key encoding".to_string())?;
        let arr: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| "Invalid public key length".to_string())?;
        if arr.iter().all(|b| *b == 0) {
            return Err("Invalid peer public key".into());
        }
        let peer = PublicKey::from(arr);
        Ok(Channel { cipher: SalsaBox::new(&peer, secret) })
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> Vec<u8> {
        let nonce = SalsaBox::generate_nonce(&mut OsRng);
        let sealed = self.cipher.encrypt(&nonce, plaintext).unwrap_or_default();
        let mut out = Vec::with_capacity(NONCE_LEN + sealed.len());
        out.extend_from_slice(nonce.as_slice());
        out.extend_from_slice(&sealed);
        out
    }

    pub fn decrypt(&self, bundle: &[u8]) -> Result<Vec<u8>, String> {
        if bundle.len() < NONCE_LEN {
            return Err("Ciphertext bundle too short".into());
        }
        let nonce = crypto_box::aead::generic_array::GenericArray::from_slice(&bundle[..NONCE_LEN]);
        self.cipher
            .decrypt(nonce, &bundle[NONCE_LEN..])
            .map_err(|_| "Decryption failed".to_string())
    }

    /// Text plaintext travels as base64 ciphertext in a text frame.
    pub fn encrypt_text(&self, text: &str) -> String {
        STANDARD.encode(self.encrypt(text.as_bytes()))
    }

    pub fn decrypt_text(&self, frame: &str) -> Result<String, String> {
        // Mirrors base64ToArrayBuffer: accept url-safe alphabet and missing padding.
        let mut s: String = frame.trim().chars().map(|c| match c { '-' => '+', '_' => '/', c => c }).collect();
        while s.len() % 4 != 0 {
            s.push('=');
        }
        let bytes = STANDARD.decode(s.as_bytes()).map_err(|e| e.to_string())?;
        let plain = self.decrypt(&bytes)?;
        String::from_utf8(plain).map_err(|e| e.to_string())
    }
}
