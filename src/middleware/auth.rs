//! Motor de autenticação: API Key, Basic Auth e JWT HS256 Stateless (RFC 7519) com tempo constante.
use crate::config::AuthConfig;
use crate::error::{GatewayError, Result};
use base64::Engine;
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default)]
pub struct AuthIdentity {
    pub subject: Option<String>,
    pub scopes: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JwtClaims {
    pub sub: Option<String>,
    pub iss: Option<String>,
    pub aud: Option<serde_json::Value>,
    pub exp: Option<u64>,
    pub nbf: Option<u64>,
    pub scope: Option<String>,
    pub scopes: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct AuthGate {
    cfg: AuthConfig,
}

impl AuthGate {
    pub fn new(cfg: AuthConfig) -> Self {
        Self { cfg }
    }

    pub fn check(&self, headers: &HeaderMap) -> Result<()> {
        self.authenticate(headers, None).map(|_| ())
    }

    pub fn authenticate(&self, headers: &HeaderMap, query: Option<&str>) -> Result<AuthIdentity> {
        match &self.cfg {
            AuthConfig::ApiKey {
                header,
                query_param,
                keys,
            } => {
                let name = header.to_ascii_lowercase();
                let header_val = headers
                    .iter()
                    .find(|(k, _)| k.as_str() == name)
                    .and_then(|(_, v)| v.to_str().ok());

                let candidate = header_val.or_else(|| {
                    let qp = query_param.as_deref()?;
                    let q = query?;
                     extract_query_param(q, qp)
                });

                match candidate {
                    Some(v) if keys.iter().any(|k| constant_time_eq(k.as_bytes(), v.as_bytes())) => {
                        Ok(AuthIdentity::default())
                    }
                    _ => Err(GatewayError::Unauthorized),
                }
            }
            AuthConfig::Basic { users } => {
                let auth = headers
                    .get(http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .ok_or(GatewayError::Unauthorized)?;
                let rest = auth
                    .strip_prefix("Basic ")
                    .or_else(|| auth.strip_prefix("basic "))
                    .ok_or(GatewayError::Unauthorized)?;
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(rest.trim())
                    .map_err(|_| GatewayError::Unauthorized)?;
                let pair = String::from_utf8(decoded).map_err(|_| GatewayError::Unauthorized)?;
                if users
                    .iter()
                    .any(|u| constant_time_eq(u.as_bytes(), pair.as_bytes()))
                {
                    let sub = pair.split(':').next().map(str::to_string);
                    Ok(AuthIdentity {
                        subject: sub,
                        scopes: Vec::new(),
                    })
                } else {
                    Err(GatewayError::Unauthorized)
                }
            }
            AuthConfig::Jwt {
                secret,
                issuer,
                audience,
                required_scopes,
            } => {
                let auth = headers
                    .get(http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .ok_or(GatewayError::Unauthorized)?;
                let token = auth
                    .strip_prefix("Bearer ")
                    .or_else(|| auth.strip_prefix("bearer "))
                    .ok_or(GatewayError::Unauthorized)?
                    .trim();

                let claims = verify_jwt_hs256(token, secret.as_bytes())?;

                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                if let Some(exp) = claims.exp {
                    if now >= exp {
                        return Err(GatewayError::Unauthorized);
                    }
                }
                if let Some(nbf) = claims.nbf {
                    if now < nbf {
                        return Err(GatewayError::Unauthorized);
                    }
                }
                if let Some(expected_iss) = issuer {
                    if claims.iss.as_deref() != Some(expected_iss.as_str()) {
                        return Err(GatewayError::Unauthorized);
                    }
                }
                if let Some(expected_aud) = audience {
                    let aud_ok = match &claims.aud {
                        Some(serde_json::Value::String(s)) => s == expected_aud,
                        Some(serde_json::Value::Array(arr)) => {
                            arr.iter().any(|v| v.as_str() == Some(expected_aud.as_str()))
                        }
                        _ => false,
                    };
                    if !aud_ok {
                        return Err(GatewayError::Unauthorized);
                    }
                }

                let mut token_scopes = Vec::new();
                if let Some(s) = &claims.scope {
                    token_scopes.extend(s.split_whitespace().map(str::to_string));
                }
                if let Some(arr) = &claims.scopes {
                    token_scopes.extend(arr.iter().cloned());
                }

                for req_scope in required_scopes {
                    if !token_scopes.iter().any(|s| s == req_scope) {
                        return Err(GatewayError::Unauthorized);
                    }
                }

                Ok(AuthIdentity {
                    subject: claims.sub,
                    scopes: token_scopes,
                })
            }
        }
    }
}

fn extract_query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    for pair in query.split('&') {
        let mut it = pair.splitn(2, '=');
        if let (Some(k), Some(v)) = (it.next(), it.next()) {
            if k == key {
                return Some(v);
            }
        }
    }
    None
}

/// Comparação em tempo constante para evitar ataques de temporização (timing attacks).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Valida e decodifica um token JWT HS256 (RFC 7519).
pub fn verify_jwt_hs256(token: &str, secret: &[u8]) -> Result<JwtClaims> {
    let mut parts = token.split('.');
    let header_b64 = parts.next().ok_or(GatewayError::Unauthorized)?;
    let payload_b64 = parts.next().ok_or(GatewayError::Unauthorized)?;
    let sig_b64 = parts.next().ok_or(GatewayError::Unauthorized)?;
    if parts.next().is_some() {
        return Err(GatewayError::Unauthorized);
    }

    let header_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(header_b64)
        .map_err(|_| GatewayError::Unauthorized)?;
    let header_json: serde_json::Value =
        serde_json::from_slice(&header_bytes).map_err(|_| GatewayError::Unauthorized)?;
    if header_json.get("alg").and_then(|v| v.as_str()) != Some("HS256") {
        return Err(GatewayError::Unauthorized);
    }

    let signing_input = format!("{header_b64}.{payload_b64}");
    let expected_sig = hmac_sha256(secret, signing_input.as_bytes());
    let got_sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|_| GatewayError::Unauthorized)?;

    if !constant_time_eq(&expected_sig, &got_sig) {
        return Err(GatewayError::Unauthorized);
    }

    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| GatewayError::Unauthorized)?;
    serde_json::from_slice(&payload_bytes).map_err(|_| GatewayError::Unauthorized)
}

/// Gera um token JWT HS256 assinado (útil para testes e emissão interna).
pub fn sign_jwt_hs256(claims: &JwtClaims, secret: &[u8]) -> String {
    let header = r#"{"alg":"HS256","typ":"JWT"}"#;
    let payload = serde_json::to_string(claims).unwrap_or_else(|_| "{}".into());
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let h_enc = b64.encode(header.as_bytes());
    let p_enc = b64.encode(payload.as_bytes());
    let signing_input = format!("{h_enc}.{p_enc}");
    let sig = hmac_sha256(secret, signing_input.as_bytes());
    let s_enc = b64.encode(sig);
    format!("{signing_input}.{s_enc}")
}

/// Implementação RFC 2104 HMAC-SHA256.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut k_block = [0u8; 64];
    if key.len() > 64 {
        let hashed = sha256(key);
        k_block[..32].copy_from_slice(&hashed);
    } else {
        k_block[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k_block[i];
        opad[i] ^= k_block[i];
    }

    let mut inner = Vec::with_capacity(64 + message.len());
    inner.extend_from_slice(&ipad);
    inner.extend_from_slice(message);
    let inner_hash = sha256(&inner);

    let mut outer = [0u8; 96];
    outer[..64].copy_from_slice(&opad);
    outer[64..].copy_from_slice(&inner_hash);
    sha256(&outer)
}

/// Implementação FIPS 180-4 / RFC 6234 SHA-256.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ];

    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];

    let bit_len = (data.len() as u64) * 8;
    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0x00);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, word) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..(i + 1) * 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderName, HeaderValue};

    #[test]
    fn sha256_nist_vector() {
        let digest = sha256(b"abc");
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn api_key_ok() {
        let gate = AuthGate::new(AuthConfig::ApiKey {
            header: "X-API-Key".into(),
            query_param: None,
            keys: vec!["secret".into()],
        });
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("secret"),
        );
        assert!(gate.check(&h).is_ok());
    }

    #[test]
    fn api_key_denied() {
        let gate = AuthGate::new(AuthConfig::ApiKey {
            header: "X-API-Key".into(),
            query_param: None,
            keys: vec!["secret".into()],
        });
        assert!(gate.check(&HeaderMap::new()).is_err());
    }

    #[test]
    fn basic_ok() {
        let gate = AuthGate::new(AuthConfig::Basic {
            users: vec!["alice:wonder".into()],
        });
        let mut h = HeaderMap::new();
        let token = base64::engine::general_purpose::STANDARD.encode("alice:wonder");
        h.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {token}")).unwrap(),
        );
        assert!(gate.check(&h).is_ok());
    }

    #[test]
    fn basic_denied() {
        let gate = AuthGate::new(AuthConfig::Basic {
            users: vec!["alice:wonder".into()],
        });
        let mut h = HeaderMap::new();
        let token = base64::engine::general_purpose::STANDARD.encode("alice:wrong");
        h.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {token}")).unwrap(),
        );
        assert!(gate.check(&h).is_err());
    }

    #[test]
    fn jwt_hs256_valid_and_expired() {
        let secret = "my-ultra-secure-jwt-secret-key-2026";
        let gate = AuthGate::new(AuthConfig::Jwt {
            secret: secret.into(),
            issuer: Some("auth.gateway".into()),
            audience: None,
            required_scopes: vec!["read:all".into()],
        });

        let valid_claims = JwtClaims {
            sub: Some("vinicius".into()),
            iss: Some("auth.gateway".into()),
            aud: None,
            exp: Some(4_000_000_000),
            nbf: None,
            scope: Some("read:all write:all".into()),
            scopes: None,
        };
        let token = sign_jwt_hs256(&valid_claims, secret.as_bytes());
        let mut h = HeaderMap::new();
        h.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let id = gate.authenticate(&h, None).unwrap();
        assert_eq!(id.subject.as_deref(), Some("vinicius"));

        // Token expirado
        let exp_claims = JwtClaims {
            exp: Some(100),
            ..valid_claims
        };
        let exp_token = sign_jwt_hs256(&exp_claims, secret.as_bytes());
        h.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {exp_token}")).unwrap(),
        );
        assert!(gate.authenticate(&h, None).is_err());
    }
}
