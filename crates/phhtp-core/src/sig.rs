//! HMAC-SHA256 message signing with constant-time verification.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Compute `HMAC-SHA256(secret, message)`.
pub fn sign(secret: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

/// Constant-time check that `signature` is `HMAC-SHA256(secret, message)`.
pub fn verify(secret: &[u8], message: &[u8], signature: &[u8]) -> bool {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(message);
    mac.verify_slice(signature).is_ok()
}

/// `rounds` chained SHA-256 digests of `seed`. A tunable CPU-bound workload.
pub fn sha256_chain(seed: &[u8], rounds: u32) -> [u8; 32] {
    let mut digest: [u8; 32] = Sha256::digest(seed).into();
    for _ in 1..rounds {
        digest = Sha256::digest(digest).into();
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc4231_test_case_2() {
        let sig = sign(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            hex::encode(sig),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn verify_roundtrip() {
        let sig = sign(b"k", b"msg");
        assert!(verify(b"k", b"msg", &sig));
        assert!(!verify(b"k", b"msg2", &sig));
        assert!(!verify(b"k2", b"msg", &sig));
        assert!(!verify(b"k", b"msg", &sig[..31]));
    }

    #[test]
    fn chain_is_deterministic() {
        assert_eq!(
            sha256_chain(b"x", 1),
            <[u8; 32]>::from(Sha256::digest(b"x"))
        );
        assert_eq!(sha256_chain(b"x", 10), sha256_chain(b"x", 10));
        assert_ne!(sha256_chain(b"x", 10), sha256_chain(b"x", 11));
    }
}
