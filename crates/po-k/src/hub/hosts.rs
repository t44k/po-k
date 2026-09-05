//! Host-name resolution for the hub.
//!
//! `connect` accepts a bare box name (`ange` → `http://ange.zrz:13658`), a
//! `host:port`, a full `http(s)://` base URL, or `local` (this po-k). The key
//! stored in `hub_hosts` is the normalised input, so an agent refers to a box
//! by whatever it connected with.

use crate::defaults;

pub const LOCAL: &str = "local";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub key: String,
    pub base_url: String,
}

fn valid_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 63
        && !label.starts_with('-')
        && !label.ends_with('-')
        && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

fn valid_hostname(host: &str) -> bool {
    !host.is_empty() && host.len() <= 253 && host.split('.').all(valid_label)
}

/// Resolve user input to a hub key + base URL. `self_url` is this po-k's own
/// callback URL, used for `local`.
pub fn resolve(input: &str, self_url: &str) -> Result<Resolved, String> {
    let raw = input.trim().to_lowercase();
    if raw.is_empty() {
        return Err("host is required".into());
    }
    if raw == LOCAL {
        return Ok(Resolved {
            key: LOCAL.into(),
            base_url: self_url.trim_end_matches('/').to_string(),
        });
    }
    if raw.starts_with("http://") || raw.starts_with("https://") {
        let base = raw.trim_end_matches('/').to_string();
        if base.contains(char::is_whitespace) || base.matches('/').count() > 2 {
            return Err(format!("host URL {input:?} must be a bare scheme://host[:port]"));
        }
        // The key must be a single URL path segment (`/hosts/{host}/...`), so
        // drop the scheme: `http://box:7071` and `box:7071` share one key.
        let key = base.split_once("://").map(|(_, rest)| rest.to_string()).unwrap_or_else(|| base.clone());
        return Ok(Resolved { key, base_url: base });
    }
    if raw.contains('/') || raw.contains(char::is_whitespace) {
        return Err(format!("host {input:?} contains invalid characters"));
    }
    let (hostname, port) = match raw.rsplit_once(':') {
        Some((h, p)) => {
            let port: u16 = p
                .parse()
                .map_err(|_| format!("host {input:?} has an invalid port {p:?}"))?;
            (h.to_string(), port)
        }
        None => (raw.clone(), defaults::remote_port()),
    };
    let is_ip = hostname.parse::<std::net::IpAddr>().is_ok();
    if !is_ip && !valid_hostname(&hostname) {
        return Err(format!("host {input:?} is not a valid DNS name"));
    }
    let fqdn = if !is_ip && !hostname.contains('.') {
        format!("{hostname}{}", defaults::host_suffix())
    } else {
        hostname
    };
    Ok(Resolved {
        key: raw,
        base_url: format!("http://{fqdn}:{port}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_label_gets_suffix_and_default_port() {
        std::env::remove_var("POK_HOST_SUFFIX");
        std::env::remove_var("POK_PORT");
        let r = resolve("Ange", "http://127.0.0.1:13658").unwrap();
        assert_eq!(r.key, "ange");
        assert_eq!(r.base_url, "http://ange.zrz:13658");
    }

    #[test]
    fn fqdn_ip_and_port_forms() {
        std::env::remove_var("POK_HOST_SUFFIX");
        std::env::remove_var("POK_PORT");
        assert_eq!(resolve("box.example.com", "").unwrap().base_url, "http://box.example.com:13658");
        assert_eq!(resolve("10.0.0.5:7071", "").unwrap().base_url, "http://10.0.0.5:7071");
        assert_eq!(resolve("127.0.0.1", "").unwrap().base_url, "http://127.0.0.1:13658");
        let url = resolve("http://127.0.0.1:7071/", "").unwrap();
        assert_eq!(url.base_url, "http://127.0.0.1:7071");
        assert_eq!(url.key, "127.0.0.1:7071");
        assert_eq!(resolve("https://box.example.com", "").unwrap().key, "box.example.com");
    }

    #[test]
    fn local_uses_self_url() {
        let r = resolve("local", "http://127.0.0.1:13658").unwrap();
        assert_eq!(r.key, "local");
        assert_eq!(r.base_url, "http://127.0.0.1:13658");
    }

    #[test]
    fn rejects_garbage() {
        for bad in ["", "a/b", "-a", "a b", "a:notaport", "http://x/sessions", &"x".repeat(64)] {
            assert!(resolve(bad, "").is_err(), "{bad:?} should be rejected");
        }
    }
}
