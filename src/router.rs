//! Motor de roteamento: Radix Trie de segmentos + Host (exato/wildcard) + Canary Weight.
use crate::config::RouteConfig;
use http::HeaderMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

static CANARY_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct RouteMatch<'a> {
    pub route: &'a RouteConfig,
    pub index: usize,
}

/// Nó da Radix Trie por segmentos de caminho para resolução rápida de prefixos.
#[derive(Debug, Default, Clone)]
pub struct RadixNode {
    children: HashMap<String, RadixNode>,
    route_indices: Vec<usize>,
}

/// Índice pré-compilado de rotas em árvore de prefixos.
#[derive(Debug, Default, Clone)]
pub struct RadixRouter {
    root: RadixNode,
}

impl RadixRouter {
    pub fn from_routes(routes: &[RouteConfig]) -> Self {
        let mut root = RadixNode::default();
        for (idx, route) in routes.iter().enumerate() {
            let segments = split_segments(&route.path_prefix);
            let mut node = &mut root;
            for seg in segments {
                node = node.children.entry(seg.to_string()).or_default();
            }
            node.route_indices.push(idx);
        }
        Self { root }
    }

    /// Coleta índices candidatos na árvore (do mais específico ao mais genérico).
    pub fn candidate_indices(&self, path: &str) -> Vec<usize> {
        let segments = split_segments(path);
        let mut node = &self.root;
        let mut matched_levels: Vec<&[usize]> = Vec::new();

        if !node.route_indices.is_empty() {
            matched_levels.push(&node.route_indices);
        }

        for seg in segments {
            if let Some(child) = node.children.get(seg) {
                node = child;
                if !node.route_indices.is_empty() {
                    matched_levels.push(&node.route_indices);
                }
            } else {
                break;
            }
        }

        let mut out = Vec::new();
        for level in matched_levels.into_iter().rev() {
            out.extend_from_slice(level);
        }
        out
    }
}

fn split_segments(path: &str) -> Vec<&str> {
    path.trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect()
}

/// Seleciona a rota compatível com host, path_prefix e canary_weight.
pub fn match_route<'a>(
    routes: &'a [RouteConfig],
    host: Option<&str>,
    path: &str,
) -> Option<RouteMatch<'a>> {
    match_route_full(routes, host, path, None, None)
}

/// Seleção avançada considerando Host, Path, Método HTTP, Headers e Canary Weight.
pub fn match_route_full<'a>(
    routes: &'a [RouteConfig],
    host: Option<&str>,
    path: &str,
    method: Option<&str>,
    headers: Option<&HeaderMap>,
) -> Option<RouteMatch<'a>> {
    let host_norm = host.map(normalize_host);
    let mut candidates: Vec<(usize, &'a RouteConfig)> = Vec::new();

    for (index, route) in routes.iter().enumerate() {
        if let Some(ref expected) = route.host {
            match &host_norm {
                Some(h) if host_matches(expected, h) => {}
                _ => continue,
            }
        }

        if !path_matches(&route.path_prefix, path) {
            continue;
        }

        if let Some(m) = method {
            if !route.methods.is_empty()
                && !route.methods.iter().any(|allowed| allowed.eq_ignore_ascii_case(m))
            {
                continue;
            }
        }

        if let Some(hdrs) = headers {
            if !headers_match(&route.match_headers, hdrs) {
                continue;
            }
        }

        // Agrupa rotas candidatas que compartilham exatamente o mesmo path_prefix e host (grupo Canary)
        if candidates.is_empty() {
            candidates.push((index, route));
        } else {
            let first = candidates[0].1;
            if first.path_prefix == route.path_prefix && first.host == route.host {
                candidates.push((index, route));
            }
        }
    }

    if candidates.is_empty() {
        return None;
    }

    if candidates.len() == 1 {
        let (index, route) = candidates[0];
        return Some(RouteMatch { route, index });
    }

    // Distribuição Canary determinística proporcional aos pesos das rotas candidatas
    let total_weight: u32 = candidates
        .iter()
        .map(|(_, r)| (r.canary_weight as u32).max(1))
        .sum();
    let seq = CANARY_COUNTER.fetch_add(1, Ordering::Relaxed);
    let ticket = (seq % total_weight as u64) as u32;

    let mut acc = 0u32;
    for (index, route) in &candidates {
        acc += (route.canary_weight as u32).max(1);
        if ticket < acc {
            return Some(RouteMatch {
                route,
                index: *index,
            });
        }
    }

    let (index, route) = candidates[0];
    Some(RouteMatch { route, index })
}

fn normalize_host(h: &str) -> String {
    let without_port = h.split(':').next().unwrap_or(h);
    without_port.to_ascii_lowercase()
}

fn host_matches(pattern: &str, actual_norm: &str) -> bool {
    let pat = normalize_host(pattern);
    if pat == "*" || pat == actual_norm {
        return true;
    }
    if let Some(suffix) = pat.strip_prefix("*.") {
        return actual_norm.ends_with(&format!(".{suffix}"));
    }
    false
}

fn headers_match(expected: &HashMap<String, String>, actual: &HeaderMap) -> bool {
    for (k, v) in expected {
        let got = actual
            .get(k.to_ascii_lowercase().as_str())
            .and_then(|val| val.to_str().ok());
        match got {
            Some(actual_val) if actual_val == v => {}
            _ => return false,
        }
    }
    true
}

fn path_matches(prefix: &str, path: &str) -> bool {
    if prefix == "/" {
        return true;
    }
    if path == prefix {
        return true;
    }
    let p = if prefix.ends_with('/') {
        prefix.trim_end_matches('/')
    } else {
        prefix
    };
    path == p || path.starts_with(&(p.to_string() + "/"))
}

/// Ajusta path ao encaminhar (`strip_prefix`).
pub fn rewrite_path(route: &RouteConfig, path: &str) -> String {
    if !route.strip_prefix {
        return path.to_string();
    }
    let prefix = route.path_prefix.trim_end_matches('/');
    if path == prefix || path == route.path_prefix {
        return "/".to_string();
    }
    if let Some(rest) = path.strip_prefix(prefix) {
        if rest.is_empty() {
            "/".into()
        } else if rest.starts_with('/') {
            rest.to_string()
        } else {
            format!("/{rest}")
        }
    } else {
        path.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LoadBalanceStrategy;

    fn route(name: &str, host: Option<&str>, prefix: &str) -> RouteConfig {
        RouteConfig {
            name: name.into(),
            host: host.map(str::to_string),
            path_prefix: prefix.into(),
            strip_prefix: false,
            methods: Vec::new(),
            match_headers: HashMap::new(),
            canary_weight: 100,
            upstreams: vec!["http://127.0.0.1:1".into()],
            algorithm: LoadBalanceStrategy::RoundRobin,
            rate_limit: None,
            auth: None,
            circuit_breaker: None,
            adaptive_concurrency: None,
            retry: None,
            cors: None,
            headers: None,
            ip_filter: None,
            max_body_bytes: None,
            timeout_ms: 1000,
        }
    }

    #[test]
    fn path_prefix_match() {
        let routes = vec![route("a", None, "/api"), route("b", None, "/")];
        let m = match_route(&routes, None, "/api/v1").unwrap();
        assert_eq!(m.route.name, "a");
        let m2 = match_route(&routes, None, "/other").unwrap();
        assert_eq!(m2.route.name, "b");
    }

    #[test]
    fn host_match() {
        let routes = vec![
            route("api", Some("api.local"), "/"),
            route("www", Some("www.local"), "/"),
            route("wild", Some("*.svc.local"), "/"),
        ];
        let m = match_route(&routes, Some("API.local:443"), "/x").unwrap();
        assert_eq!(m.route.name, "api");
        let m_wild = match_route(&routes, Some("orders.svc.local"), "/x").unwrap();
        assert_eq!(m_wild.route.name, "wild");
        assert!(match_route(&routes, Some("other.local"), "/x").is_none());
    }

    #[test]
    fn strip_prefix() {
        let mut r = route("a", None, "/api");
        r.strip_prefix = true;
        assert_eq!(rewrite_path(&r, "/api"), "/");
        assert_eq!(rewrite_path(&r, "/api/users"), "/users");
        r.strip_prefix = false;
        assert_eq!(rewrite_path(&r, "/api/users"), "/api/users");
    }

    #[test]
    fn exact_prefix_boundary() {
        let routes = vec![route("a", None, "/api")];
        assert!(match_route(&routes, None, "/api").is_some());
        assert!(match_route(&routes, None, "/api/").is_some());
        assert!(match_route(&routes, None, "/apitest").is_none());
    }

    #[test]
    fn canary_weighted_split() {
        let mut r1 = route("v1", None, "/app");
        r1.canary_weight = 80;
        let mut r2 = route("v2", None, "/app");
        r2.canary_weight = 20;
        let routes = vec![r1, r2];

        let mut v1_hits = 0;
        let mut v2_hits = 0;
        for _ in 0..100 {
            let m = match_route(&routes, None, "/app/test").unwrap();
            if m.route.name == "v1" {
                v1_hits += 1;
            } else {
                v2_hits += 1;
            }
        }
        assert_eq!(v1_hits, 80);
        assert_eq!(v2_hits, 20);
    }
}
