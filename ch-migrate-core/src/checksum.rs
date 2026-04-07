use sha2::{Digest, Sha256};

/// Compute SHA-256 checksum of migration SQL content. Returns 32 bytes.
pub fn compute(sql: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(sql.as_bytes());
    hasher.finalize().to_vec()
}

/// Convert checksum bytes to hex string for storage/display.
pub fn to_hex(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

/// Parse hex string back to bytes.
pub fn from_hex(hex_str: &str) -> Result<Vec<u8>, hex::FromHexError> {
    hex::decode(hex_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_deterministic() {
        let sql = "CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY id";
        let a = compute(sql);
        let b = compute(sql);
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
    }

    #[test]
    fn checksum_differs_for_different_input() {
        let a = compute("SELECT 1");
        let b = compute("SELECT 2");
        assert_ne!(a, b);
    }

    #[test]
    fn hex_roundtrip() {
        let bytes = compute("test");
        let hex_str = to_hex(&bytes);
        assert_eq!(hex_str.len(), 64);
        let decoded = from_hex(&hex_str).unwrap();
        assert_eq!(bytes, decoded);
    }
}
