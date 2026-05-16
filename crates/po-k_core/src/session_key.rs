use serde::{Deserialize, Serialize};

/// Stable identity for a Claude Code session, scoped to a specific machine so two
/// collectors that happen to share a session UUID (re-cloned repo, copy-paste, etc.)
/// don't collide on the server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct SessionKey(String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct MachineId(String);

impl MachineId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for MachineId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for MachineId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl SessionKey {
    pub fn derive(machine: &MachineId, sanitized_cwd: &str, session_uuid: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(machine.0.as_bytes());
        hasher.update(b":");
        hasher.update(sanitized_cwd.as_bytes());
        hasher.update(b":");
        hasher.update(session_uuid.as_bytes());
        Self(hasher.finalize().to_hex().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_is_deterministic_and_machine_scoped() {
        let m1 = MachineId::from("m1");
        let m2 = MachineId::from("m2");
        let a = SessionKey::derive(&m1, "-workspace", "uuid-x");
        let b = SessionKey::derive(&m1, "-workspace", "uuid-x");
        let c = SessionKey::derive(&m2, "-workspace", "uuid-x");
        assert_eq!(a, b);
        assert_ne!(a, c);
        // 64 hex chars (256-bit blake3).
        assert_eq!(a.as_str().len(), 64);
    }
}
