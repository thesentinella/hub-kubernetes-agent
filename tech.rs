//! Detect container "technology" from its image string.
//!
//! Strategy: split off registry/repo, then match the final image name (and tag) against
//! a small table of well-known products. This is intentionally simple — the truth source
//! is the image, and ambiguous matches just fall through.
//!
//! Examples:
//!   nginx:1.25-alpine                                  -> nginx 1.25
//!   docker.io/library/postgres:15.4                    -> postgres 15.4
//!   quay.io/keycloak/keycloak:24.0.1                   -> keycloak 24.0.1
//!   registry.k8s.io/kube-apiserver:v1.30.2             -> kubernetes-apiserver 1.30.2
//!   eclipse-temurin:21-jre                             -> java 21

use crate::model::Technology;

pub fn detect(image: &str) -> Technology {
    // Strip registry host (anything with a '.' or ':' before the first '/').
    let after_registry = strip_registry(image);

    // Split tag (handle digests too: image@sha256:... -> ignore digest as version).
    let (repo, tag) = split_tag(after_registry);

    // Take the last path segment as the canonical image name.
    let name = repo.rsplit('/').next().unwrap_or(repo).to_lowercase();

    // Match against the rules table.
    for rule in RULES {
        if rule.matches(&name) {
            return Technology {
                vendor: Some(rule.vendor.to_string()),
                product: Some(rule.product.to_string()),
                version: tag.map(normalize_version),
                source: "image",
            };
        }
    }

    // Unknown — return the raw image name as product so the Hub still has something to group by.
    Technology {
        vendor: None,
        product: Some(name),
        version: tag.map(normalize_version),
        source: "image",
    }
}

fn strip_registry(image: &str) -> &str {
    match image.split_once('/') {
        Some((head, rest)) if head.contains('.') || head.contains(':') || head == "localhost" => {
            rest
        }
        _ => image,
    }
}

fn split_tag(repo_with_tag: &str) -> (&str, Option<&str>) {
    if let Some((repo, _digest)) = repo_with_tag.split_once('@') {
        return (repo, None); // digest pinned, no human version
    }
    if let Some((repo, tag)) = repo_with_tag.rsplit_once(':') {
        // Make sure the colon isn't part of a port in a registry we missed.
        if !tag.contains('/') {
            return (repo, Some(tag));
        }
    }
    (repo_with_tag, None)
}

/// Strip leading 'v' and common suffixes like "-alpine", "-jre", "-slim".
fn normalize_version(tag: &str) -> String {
    let mut v = tag.trim_start_matches('v').to_string();
    for suffix in ["-alpine", "-slim", "-jre", "-jdk", "-bullseye", "-bookworm"] {
        if let Some(stripped) = v.strip_suffix(suffix) {
            v = stripped.to_string();
        }
    }
    v
}

struct Rule {
    vendor: &'static str,
    product: &'static str,
    /// Match if the image name equals any of these, OR starts_with any prefix.
    names: &'static [&'static str],
    prefixes: &'static [&'static str],
}

impl Rule {
    fn matches(&self, name: &str) -> bool {
        if self.names.iter().any(|n| *n == name) {
            return true;
        }
        self.prefixes.iter().any(|p| name.starts_with(p))
    }
}

const RULES: &[Rule] = &[
    // Web / proxy
    Rule { vendor: "nginx",     product: "nginx",     names: &["nginx", "nginx-unprivileged"], prefixes: &[] },
    Rule { vendor: "apache",    product: "httpd",     names: &["httpd"], prefixes: &[] },
    Rule { vendor: "haproxy",   product: "haproxy",   names: &["haproxy"], prefixes: &[] },
    Rule { vendor: "envoy",     product: "envoy",     names: &["envoy"], prefixes: &[] },
    Rule { vendor: "traefik",   product: "traefik",   names: &["traefik"], prefixes: &[] },
    Rule { vendor: "caddy",     product: "caddy",     names: &["caddy"], prefixes: &[] },

    // Databases
    Rule { vendor: "postgresql", product: "postgres", names: &["postgres", "postgresql"], prefixes: &["postgres-"] },
    Rule { vendor: "mysql",     product: "mysql",     names: &["mysql"], prefixes: &[] },
    Rule { vendor: "mariadb",   product: "mariadb",   names: &["mariadb"], prefixes: &[] },
    Rule { vendor: "mongodb",   product: "mongodb",   names: &["mongo", "mongodb"], prefixes: &[] },
    Rule { vendor: "redis",     product: "redis",     names: &["redis"], prefixes: &[] },
    Rule { vendor: "elastic",   product: "elasticsearch", names: &["elasticsearch"], prefixes: &[] },
    Rule { vendor: "elastic",   product: "kibana",    names: &["kibana"], prefixes: &[] },
    Rule { vendor: "influxdata", product: "influxdb", names: &["influxdb"], prefixes: &[] },
    Rule { vendor: "cockroachdb", product: "cockroachdb", names: &["cockroach"], prefixes: &[] },

    // Messaging / streaming
    Rule { vendor: "rabbitmq",  product: "rabbitmq",  names: &["rabbitmq"], prefixes: &[] },
    Rule { vendor: "apache",    product: "kafka",     names: &["kafka"], prefixes: &["cp-kafka", "confluent"] },
    Rule { vendor: "nats",      product: "nats",      names: &["nats"], prefixes: &[] },

    // Runtimes / language base images
    Rule { vendor: "oracle",    product: "java",      names: &["openjdk"], prefixes: &["eclipse-temurin", "amazoncorretto", "ibm-semeru-runtimes"] },
    Rule { vendor: "nodejs",    product: "node",      names: &["node"], prefixes: &[] },
    Rule { vendor: "python",    product: "python",    names: &["python"], prefixes: &[] },
    Rule { vendor: "golang",    product: "go",        names: &["golang"], prefixes: &[] },
    Rule { vendor: "rust-lang", product: "rust",      names: &["rust"], prefixes: &[] },
    Rule { vendor: "microsoft", product: "dotnet",    names: &[], prefixes: &["dotnet"] },
    Rule { vendor: "ruby",      product: "ruby",      names: &["ruby"], prefixes: &[] },
    Rule { vendor: "php",       product: "php",       names: &["php"], prefixes: &[] },

    // Observability
    Rule { vendor: "prometheus", product: "prometheus", names: &["prometheus"], prefixes: &[] },
    Rule { vendor: "grafana",   product: "grafana",   names: &["grafana"], prefixes: &[] },
    Rule { vendor: "elastic",   product: "logstash",  names: &["logstash"], prefixes: &[] },
    Rule { vendor: "fluent",    product: "fluentd",   names: &["fluentd", "fluent-bit"], prefixes: &[] },

    // Kubernetes / OpenShift control plane and components
    Rule { vendor: "kubernetes", product: "kube-apiserver",          names: &["kube-apiserver"],          prefixes: &[] },
    Rule { vendor: "kubernetes", product: "kube-controller-manager", names: &["kube-controller-manager"], prefixes: &[] },
    Rule { vendor: "kubernetes", product: "kube-scheduler",          names: &["kube-scheduler"],          prefixes: &[] },
    Rule { vendor: "kubernetes", product: "kube-proxy",              names: &["kube-proxy"],              prefixes: &[] },
    Rule { vendor: "kubernetes", product: "coredns",                 names: &["coredns"],                 prefixes: &[] },
    Rule { vendor: "kubernetes", product: "etcd",                    names: &["etcd"],                    prefixes: &[] },
    Rule { vendor: "openshift",  product: "ose",                     names: &[],                          prefixes: &["ose-"] },

    // Sidecars common in service mesh
    Rule { vendor: "istio",     product: "istio-proxy", names: &["proxyv2"], prefixes: &[] },
    Rule { vendor: "linkerd",   product: "linkerd-proxy", names: &["proxy"], prefixes: &["linkerd2-proxy"] },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_nginx_alpine() {
        let t = detect("nginx:1.25-alpine");
        assert_eq!(t.product.as_deref(), Some("nginx"));
        assert_eq!(t.version.as_deref(), Some("1.25"));
    }

    #[test]
    fn detects_postgres_with_registry() {
        let t = detect("docker.io/library/postgres:15.4");
        assert_eq!(t.product.as_deref(), Some("postgres"));
        assert_eq!(t.version.as_deref(), Some("15.4"));
    }

    #[test]
    fn detects_kube_apiserver() {
        let t = detect("registry.k8s.io/kube-apiserver:v1.30.2");
        assert_eq!(t.vendor.as_deref(), Some("kubernetes"));
        assert_eq!(t.version.as_deref(), Some("1.30.2"));
    }

    #[test]
    fn handles_digest_pinned() {
        let t = detect("nginx@sha256:abc123");
        assert_eq!(t.product.as_deref(), Some("nginx"));
        assert_eq!(t.version, None);
    }

    #[test]
    fn unknown_falls_back_to_name() {
        let t = detect("mycompany/internal-tool:2.0");
        assert_eq!(t.vendor, None);
        assert_eq!(t.product.as_deref(), Some("internal-tool"));
        assert_eq!(t.version.as_deref(), Some("2.0"));
    }

    #[test]
    fn handles_registry_with_port() {
        let t = detect("registry.local:5000/team/redis:7.2");
        assert_eq!(t.product.as_deref(), Some("redis"));
        assert_eq!(t.version.as_deref(), Some("7.2"));
    }
}
