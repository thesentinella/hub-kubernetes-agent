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
    let repo_lower = repo.to_lowercase();

    if repo_lower.starts_with("distroless/") || repo_lower.contains("/distroless/") {
        return Technology {
            vendor: Some("distroless".to_string()),
            product: Some("distroless".to_string()),
            version: tag.map(normalize_version),
            language: None,
            source: "image",
            subtype: None,
        };
    }

    // Take the last path segment as the canonical image name.
    let name = repo.rsplit('/').next().unwrap_or(repo).to_lowercase();

    // Match against the rules table.
    for rule in RULES {
        if rule.matches(&name) {
            let subtype = if rule.product == "postgres" {
                Some("postgresql".to_string())
            } else {
                None
            };
            return Technology {
                vendor: Some(rule.vendor.to_string()),
                product: Some(rule.product.to_string()),
                version: tag.map(normalize_version),
                language: if rule.language.is_empty() {
                    None
                } else {
                    Some(rule.language.to_string())
                },
                source: "image",
                subtype,
            };
        }
    }

    // Unknown — return the raw image name as product so the Hub still has something to group by.
    Technology {
        vendor: None,
        product: Some(name),
        version: tag.map(normalize_version),
        language: None,
        source: "image",
        subtype: None,
    }
}

/// Detect Angular, Spring Boot, and Oracle from image path patterns that do
/// not match the simple name/prefix rule table. Returns `Some(Technology)`
/// when the image repo path itself encodes the application stack.
pub fn detect_application_stack_from_image(image: &str) -> Option<Technology> {
    let lower_full = image.to_lowercase();
    let (repo_no_tag, _tag) = split_tag(image);
    let repo_lower = strip_registry(repo_no_tag).to_lowercase();
    let name = repo_lower.rsplit('/').next().unwrap_or(repo_lower.as_str());

    // Oracle Database on container-registry.oracle.com. Check the original
    // image string so the registry host is preserved (strip_registry removes
    // the leading host segment).
    if lower_full.starts_with("container-registry.oracle.com/database/") {
        return Some(Technology {
            vendor: Some("oracle".to_string()),
            product: Some("oracle-database".to_string()),
            version: None,
            language: None,
            source: "image",
            subtype: Some("oracle_database".to_string()),
        });
    }

    // gvenzl/oracle-xe / oracle-free (registry stripped)
    if repo_lower.starts_with("gvenzl/oracle-") {
        return Some(Technology {
            vendor: Some("gvenzl".to_string()),
            product: Some("oracle-database".to_string()),
            version: None,
            language: None,
            source: "image",
            subtype: Some("oracle_database".to_string()),
        });
    }

    // Oracle (image name "oracle/database")
    if repo_lower == "oracle/database" || repo_lower.ends_with("/oracle/database") {
        return Some(Technology {
            vendor: Some("oracle".to_string()),
            product: Some("oracle-database".to_string()),
            version: None,
            language: None,
            source: "image",
            subtype: Some("oracle_database".to_string()),
        });
    }

    // Spring Boot fat-jar image convention
    if repo_lower.contains("springboot") || repo_lower.contains("spring-boot") {
        return Some(Technology {
            vendor: None,
            product: Some("spring-boot".to_string()),
            version: None,
            language: Some("Java".to_string()),
            source: "image",
            subtype: Some("spring_boot".to_string()),
        });
    }

    // Angular runtime images: anything starting with `angular-` or `*-ng-`
    // in the image name. Tag the workload as Angular; the runtime product
    // (typically nginx) is detected separately by the normal image rules.
    if name.starts_with("angular-") || name.contains("-ng-") || name.starts_with("ng-") {
        return Some(Technology {
            vendor: None,
            product: Some("angular".to_string()),
            version: None,
            language: None,
            source: "image",
            subtype: Some("angular".to_string()),
        });
    }

    None
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
    for suffix in [
        "-alpine",
        "-slim",
        "-jre",
        "-jdk",
        "-bullseye",
        "-bookworm",
        "-distroless",
        "-ubi",
        "-ubi8",
        "-ubi9",
    ] {
        if let Some(stripped) = v.strip_suffix(suffix) {
            v = stripped.to_string();
        }
    }
    v
}

struct Rule {
    vendor: &'static str,
    product: &'static str,
    language: &'static str,
    /// Match if the image name equals any of these, OR starts_with any prefix.
    names: &'static [&'static str],
    prefixes: &'static [&'static str],
}

impl Rule {
    fn matches(&self, name: &str) -> bool {
        if self.names.contains(&name) {
            return true;
        }
        self.prefixes.iter().any(|p| name.starts_with(p))
    }
}

const RULES: &[Rule] = &[
    // Web / proxy
    Rule {
        vendor: "nginx",
        product: "nginx",
        language: "C",
        names: &["nginx", "nginx-unprivileged"],
        prefixes: &[],
    },
    Rule {
        vendor: "apache",
        product: "httpd",
        language: "C",
        names: &["httpd"],
        prefixes: &[],
    },
    Rule {
        vendor: "haproxy",
        product: "haproxy",
        language: "C",
        names: &["haproxy"],
        prefixes: &[],
    },
    Rule {
        vendor: "envoy",
        product: "envoy",
        language: "C++",
        names: &["envoy"],
        prefixes: &[],
    },
    Rule {
        vendor: "traefik",
        product: "traefik",
        language: "Go",
        names: &["traefik"],
        prefixes: &[],
    },
    Rule {
        vendor: "caddy",
        product: "caddy",
        language: "Go",
        names: &["caddy"],
        prefixes: &[],
    },
    // Databases
    Rule {
        vendor: "postgresql",
        product: "postgres",
        language: "C",
        names: &["postgres", "postgresql"],
        prefixes: &["postgres-"],
    },
    Rule {
        vendor: "mysql",
        product: "mysql",
        language: "C++",
        names: &["mysql"],
        prefixes: &[],
    },
    Rule {
        vendor: "mariadb",
        product: "mariadb",
        language: "C++",
        names: &["mariadb"],
        prefixes: &[],
    },
    Rule {
        vendor: "mongodb",
        product: "mongodb",
        language: "C++",
        names: &["mongo", "mongodb"],
        prefixes: &[],
    },
    Rule {
        vendor: "redis",
        product: "redis",
        language: "C",
        names: &["redis"],
        prefixes: &[],
    },
    Rule {
        vendor: "elastic",
        product: "elasticsearch",
        language: "Java",
        names: &["elasticsearch"],
        prefixes: &[],
    },
    Rule {
        vendor: "elastic",
        product: "kibana",
        language: "JavaScript",
        names: &["kibana"],
        prefixes: &[],
    },
    Rule {
        vendor: "influxdata",
        product: "influxdb",
        language: "Go",
        names: &["influxdb"],
        prefixes: &[],
    },
    Rule {
        vendor: "cockroachdb",
        product: "cockroachdb",
        language: "Go",
        names: &["cockroach"],
        prefixes: &[],
    },
    // Messaging / streaming
    Rule {
        vendor: "rabbitmq",
        product: "rabbitmq",
        language: "Erlang",
        names: &["rabbitmq"],
        prefixes: &[],
    },
    Rule {
        vendor: "apache",
        product: "kafka",
        language: "Java",
        names: &["kafka"],
        prefixes: &["cp-kafka", "confluent"],
    },
    Rule {
        vendor: "nats",
        product: "nats",
        language: "Go",
        names: &["nats"],
        prefixes: &[],
    },
    Rule {
        vendor: "apache",
        product: "zookeeper",
        language: "Java",
        names: &["zookeeper"],
        prefixes: &[],
    },
    Rule {
        vendor: "memcached",
        product: "memcached",
        language: "C",
        names: &["memcached"],
        prefixes: &[],
    },
    Rule {
        vendor: "keycloak",
        product: "keycloak",
        language: "Java",
        names: &["keycloak"],
        prefixes: &[],
    },
    Rule {
        vendor: "hashicorp",
        product: "vault",
        language: "Go",
        names: &["vault"],
        prefixes: &[],
    },
    Rule {
        vendor: "hashicorp",
        product: "consul",
        language: "Go",
        names: &["consul"],
        prefixes: &[],
    },
    // Runtimes / language base images
    Rule {
        vendor: "oracle",
        product: "java",
        language: "Java",
        names: &["openjdk"],
        prefixes: &["eclipse-temurin", "amazoncorretto", "ibm-semeru-runtimes"],
    },
    Rule {
        vendor: "nodejs",
        product: "node",
        language: "JavaScript",
        names: &["node"],
        prefixes: &[],
    },
    Rule {
        vendor: "python",
        product: "python",
        language: "Python",
        names: &["python"],
        prefixes: &[],
    },
    Rule {
        vendor: "golang",
        product: "go",
        language: "Go",
        names: &["golang"],
        prefixes: &[],
    },
    Rule {
        vendor: "rust-lang",
        product: "rust",
        language: "Rust",
        names: &["rust"],
        prefixes: &[],
    },
    Rule {
        vendor: "microsoft",
        product: "dotnet",
        language: "C#",
        names: &[],
        prefixes: &["dotnet"],
    },
    Rule {
        vendor: "ruby",
        product: "ruby",
        language: "Ruby",
        names: &["ruby"],
        prefixes: &[],
    },
    Rule {
        vendor: "php",
        product: "php",
        language: "PHP",
        names: &["php"],
        prefixes: &[],
    },
    Rule {
        vendor: "busybox",
        product: "busybox",
        language: "C",
        names: &["busybox"],
        prefixes: &[],
    },
    Rule {
        vendor: "alpine",
        product: "alpine",
        language: "",
        names: &["alpine"],
        prefixes: &[],
    },
    Rule {
        vendor: "debian",
        product: "debian",
        language: "",
        names: &["debian"],
        prefixes: &[],
    },
    Rule {
        vendor: "ubuntu",
        product: "ubuntu",
        language: "",
        names: &["ubuntu"],
        prefixes: &[],
    },
    Rule {
        vendor: "distroless",
        product: "distroless",
        language: "",
        names: &["distroless"],
        prefixes: &[],
    },
    // Observability
    Rule {
        vendor: "prometheus",
        product: "prometheus",
        language: "Go",
        names: &["prometheus"],
        prefixes: &[],
    },
    Rule {
        vendor: "grafana",
        product: "grafana",
        language: "Go",
        names: &["grafana"],
        prefixes: &[],
    },
    Rule {
        vendor: "grafana",
        product: "loki",
        language: "Go",
        names: &["loki"],
        prefixes: &[],
    },
    Rule {
        vendor: "grafana",
        product: "promtail",
        language: "Go",
        names: &["promtail"],
        prefixes: &[],
    },
    Rule {
        vendor: "opentelemetry",
        product: "otel-collector",
        language: "Go",
        names: &["otelcol", "opentelemetry-collector"],
        prefixes: &[],
    },
    Rule {
        vendor: "elastic",
        product: "logstash",
        language: "Java",
        names: &["logstash"],
        prefixes: &[],
    },
    Rule {
        vendor: "fluent",
        product: "fluentd",
        language: "Ruby",
        names: &["fluentd"],
        prefixes: &[],
    },
    Rule {
        vendor: "fluent",
        product: "fluent-bit",
        language: "C",
        names: &["fluent-bit"],
        prefixes: &[],
    },
    // Kubernetes / OpenShift control plane and components
    Rule {
        vendor: "kubernetes",
        product: "kube-apiserver",
        language: "Go",
        names: &["kube-apiserver"],
        prefixes: &[],
    },
    Rule {
        vendor: "kubernetes",
        product: "kube-controller-manager",
        language: "Go",
        names: &["kube-controller-manager"],
        prefixes: &[],
    },
    Rule {
        vendor: "kubernetes",
        product: "kube-scheduler",
        language: "Go",
        names: &["kube-scheduler"],
        prefixes: &[],
    },
    Rule {
        vendor: "kubernetes",
        product: "kube-proxy",
        language: "Go",
        names: &["kube-proxy"],
        prefixes: &[],
    },
    Rule {
        vendor: "kubernetes",
        product: "coredns",
        language: "Go",
        names: &["coredns"],
        prefixes: &[],
    },
    Rule {
        vendor: "kubernetes",
        product: "etcd",
        language: "Go",
        names: &["etcd"],
        prefixes: &[],
    },
    Rule {
        vendor: "openshift",
        product: "ose",
        language: "Go",
        names: &[],
        prefixes: &["ose-"],
    },
    // Sidecars common in service mesh
    Rule {
        vendor: "istio",
        product: "istio-proxy",
        language: "C++",
        names: &["proxyv2"],
        prefixes: &[],
    },
    Rule {
        vendor: "linkerd",
        product: "linkerd-proxy",
        language: "Rust",
        names: &["proxy"],
        prefixes: &["linkerd2-proxy"],
    },
    Rule {
        vendor: "jaeger",
        product: "jaeger-agent",
        language: "Go",
        names: &["jaeger-agent"],
        prefixes: &[],
    },
    Rule {
        vendor: "jaeger",
        product: "jaeger-collector",
        language: "Go",
        names: &["jaeger-collector"],
        prefixes: &[],
    },
    Rule {
        vendor: "jaeger",
        product: "jaeger-query",
        language: "Go",
        names: &["jaeger-query"],
        prefixes: &[],
    },
];

struct ProcessRule {
    executable: &'static str,
    vendor: &'static str,
    product: &'static str,
    language: &'static str,
}

const PROCESS_RULES: &[ProcessRule] = &[
    ProcessRule {
        executable: "java",
        vendor: "eclipse",
        product: "java",
        language: "Java",
    },
    ProcessRule {
        executable: "node",
        vendor: "openjs",
        product: "nodejs",
        language: "JavaScript",
    },
    ProcessRule {
        executable: "python",
        vendor: "python-software-foundation",
        product: "python",
        language: "Python",
    },
    ProcessRule {
        executable: "python3",
        vendor: "python-software-foundation",
        product: "python",
        language: "Python",
    },
    ProcessRule {
        executable: "ruby",
        vendor: "",
        product: "ruby",
        language: "Ruby",
    },
    ProcessRule {
        executable: "php",
        vendor: "",
        product: "php",
        language: "PHP",
    },
    ProcessRule {
        executable: "php-fpm",
        vendor: "",
        product: "php-fpm",
        language: "PHP",
    },
    ProcessRule {
        executable: "nginx",
        vendor: "nginx",
        product: "nginx",
        language: "C",
    },
    ProcessRule {
        executable: "httpd",
        vendor: "apache",
        product: "httpd",
        language: "C",
    },
    ProcessRule {
        executable: "apache2",
        vendor: "apache",
        product: "httpd",
        language: "C",
    },
    ProcessRule {
        executable: "haproxy",
        vendor: "haproxy",
        product: "haproxy",
        language: "C",
    },
    ProcessRule {
        executable: "envoy",
        vendor: "envoy-proxy",
        product: "envoy",
        language: "C++",
    },
    ProcessRule {
        executable: "traefik",
        vendor: "traefik",
        product: "traefik",
        language: "Go",
    },
    ProcessRule {
        executable: "caddy",
        vendor: "caddy",
        product: "caddy",
        language: "Go",
    },
    ProcessRule {
        executable: "redis-server",
        vendor: "redis-labs",
        product: "redis",
        language: "C",
    },
    ProcessRule {
        executable: "postgres",
        vendor: "postgresql",
        product: "postgres",
        language: "C",
    },
    ProcessRule {
        executable: "postmaster",
        vendor: "postgresql",
        product: "postgres",
        language: "C",
    },
    ProcessRule {
        executable: "mysqld",
        vendor: "oracle",
        product: "mysql",
        language: "C++",
    },
    ProcessRule {
        executable: "mysqld_safe",
        vendor: "oracle",
        product: "mysql",
        language: "C++",
    },
    ProcessRule {
        executable: "mariadbd",
        vendor: "mariadb",
        product: "mariadb",
        language: "C++",
    },
    ProcessRule {
        executable: "mongod",
        vendor: "mongodb",
        product: "mongodb",
        language: "C++",
    },
    ProcessRule {
        executable: "rabbitmq-server",
        vendor: "vmware",
        product: "rabbitmq",
        language: "Erlang",
    },
    // Spring Boot: java -jar *spring*.jar or -Dspring.profiles.active=... is
    // detected via the existing "java" rule. We tag it as spring_boot via
    // the `subtype` field by inspecting the arg list in detect_from_process.
    ProcessRule {
        executable: "oracle",
        vendor: "oracle",
        product: "oracle-database",
        language: "C",
    },
    ProcessRule {
        executable: "dbca",
        vendor: "oracle",
        product: "oracle-database",
        language: "C",
    },
    ProcessRule {
        executable: "sqlplus",
        vendor: "oracle",
        product: "oracle-database",
        language: "C",
    },
];

// Spring Boot jar-name marker. Lowercase substring match against any arg.
const SPRING_BOOT_JAR_MARKERS: &[&str] = &["spring", "springboot"];
const SPRING_BOOT_FLAG_MARKERS: &[&str] = &["-dspring.profiles.active", "spring.profiles.active"];

/// Inspect the resolved `Technology` and refine it with a Spring Boot subtype
/// when the process is `java`/`java -jar` and the args reference a Spring
/// artifact or a `-Dspring.*` flag. Pure function, no side effects.
pub fn refine_spring_boot(tech: Technology, command: &[String], args: &[String]) -> Technology {
    let is_java = tech
        .product
        .as_deref()
        .map(|p| p == "java")
        .unwrap_or(false)
        || command.iter().any(|entry| {
            entry
                .rsplit('/')
                .next()
                .map(|n| n == "java")
                .unwrap_or(false)
        });
    if !is_java {
        return tech;
    }
    let haystack: Vec<&str> = command
        .iter()
        .chain(args.iter())
        .map(|s| s.as_str())
        .collect();
    let has_marker = haystack.iter().any(|arg| {
        let lower = arg.to_lowercase();
        SPRING_BOOT_JAR_MARKERS.iter().any(|m| lower.contains(m))
            || SPRING_BOOT_FLAG_MARKERS.iter().any(|m| lower.contains(m))
    });
    if has_marker {
        let mut refined = tech;
        refined.subtype = Some("spring_boot".to_string());
        refined
    } else {
        tech
    }
}

pub fn detect_from_process(command: &[String], args: &[String]) -> Option<Technology> {
    let executables: Vec<&str> = command
        .iter()
        .chain(args.iter())
        .filter_map(|entry| entry.rsplit('/').next().filter(|name| !name.is_empty()))
        .collect();

    for exe in &executables {
        for rule in PROCESS_RULES {
            if *exe == rule.executable {
                let version = extract_process_version(args);
                let subtype = if rule.product == "postgres" {
                    Some("postgresql".to_string())
                } else {
                    None
                };
                return Some(Technology {
                    vendor: if rule.vendor.is_empty() {
                        None
                    } else {
                        Some(rule.vendor.to_string())
                    },
                    product: Some(rule.product.to_string()),
                    version: version.map(normalize_version),
                    language: if rule.language.is_empty() {
                        None
                    } else {
                        Some(rule.language.to_string())
                    },
                    source: "process",
                    subtype,
                });
            }
        }
    }

    None
}

fn extract_process_version(args: &[String]) -> Option<&str> {
    for arg in args {
        if let Some(version_part) = arg.strip_prefix("--version=") {
            let v = version_part.trim();
            if v.is_empty() {
                continue;
            }
            if is_version_like(v) {
                return Some(v);
            }
        }
        if let Some(version_part) = arg.strip_prefix("-version=") {
            let v = version_part.trim();
            if v.is_empty() {
                continue;
            }
            if is_version_like(v) {
                return Some(v);
            }
        }
    }

    for pair in args.windows(2) {
        if pair[0] == "--version" || pair[0] == "-version" {
            let v = pair[1].trim();
            if v.is_empty() {
                continue;
            }
            if is_version_like(v) {
                return Some(v);
            }
        }
    }

    None
}

fn is_version_like(value: &str) -> bool {
    let v = value.trim();
    if v.is_empty() {
        return false;
    }
    v.chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_')
        && v.starts_with(|ch: char| ch.is_ascii_digit())
}

/// Detect the application stack from Kubernetes pod or workload labels and
/// annotations. Returns `Some(Technology)` when one of the known markers is
/// present, with `source = "labels"`. Subtype follows the same convention as
/// image-based detection: `angular`, `spring_boot`, `oracle_database`.
///
/// Recognized markers:
/// - `app.kubernetes.io/component` in `{angular, spring-boot, oracle}`
/// - annotation `angular.io/version`
/// - annotation `app.spring.io/version`
#[allow(dead_code)]
pub fn detect_from_labels(
    labels: &[(&str, &str)],
    annotations: &[(&str, &str)],
) -> Option<Technology> {
    for (key, value) in labels {
        if *key != "app.kubernetes.io/component" {
            continue;
        }
        let v = value.to_lowercase();
        if v == "postgres" || v == "postgresql" {
            return Some(Technology {
                vendor: Some("postgresql".to_string()),
                product: Some("postgres".to_string()),
                version: None,
                language: Some("C".to_string()),
                source: "labels",
                subtype: Some("postgresql".to_string()),
            });
        }
        if v == "angular" {
            return Some(Technology {
                vendor: None,
                product: Some("angular".to_string()),
                version: None,
                language: None,
                source: "labels",
                subtype: Some("angular".to_string()),
            });
        }
        if v == "spring-boot" || v == "spring_boot" || v == "springboot" {
            return Some(Technology {
                vendor: None,
                product: Some("spring-boot".to_string()),
                version: None,
                language: Some("Java".to_string()),
                source: "labels",
                subtype: Some("spring_boot".to_string()),
            });
        }
        if v == "oracle" || v == "oracle-database" || v == "oracle_database" {
            return Some(Technology {
                vendor: Some("oracle".to_string()),
                product: Some("oracle-database".to_string()),
                version: None,
                language: None,
                source: "labels",
                subtype: Some("oracle_database".to_string()),
            });
        }
    }

    for (key, value) in annotations {
        if *key == "angular.io/version" {
            return Some(Technology {
                vendor: None,
                product: Some("angular".to_string()),
                version: Some(value.trim().to_string()),
                language: None,
                source: "labels",
                subtype: Some("angular".to_string()),
            });
        }
        if *key == "app.spring.io/version" {
            return Some(Technology {
                vendor: None,
                product: Some("spring-boot".to_string()),
                version: Some(value.trim().to_string()),
                language: Some("Java".to_string()),
                source: "labels",
                subtype: Some("spring_boot".to_string()),
            });
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_nginx_alpine() {
        let t = detect("nginx:1.25-alpine");
        assert_eq!(t.product.as_deref(), Some("nginx"));
        assert_eq!(t.version.as_deref(), Some("1.25"));
        assert_eq!(t.language.as_deref(), Some("C"));
    }

    #[test]
    fn detects_postgres_with_registry() {
        let t = detect("docker.io/library/postgres:15.4");
        assert_eq!(t.product.as_deref(), Some("postgres"));
        assert_eq!(t.version.as_deref(), Some("15.4"));
        assert_eq!(t.language.as_deref(), Some("C"));
        assert_eq!(t.subtype.as_deref(), Some("postgresql"));
    }

    #[test]
    fn detects_kube_apiserver() {
        let t = detect("registry.k8s.io/kube-apiserver:v1.30.2");
        assert_eq!(t.vendor.as_deref(), Some("kubernetes"));
        assert_eq!(t.version.as_deref(), Some("1.30.2"));
        assert_eq!(t.language.as_deref(), Some("Go"));
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
        assert_eq!(t.language, None);
    }

    #[test]
    fn handles_registry_with_port() {
        let t = detect("registry.local:5000/team/redis:7.2");
        assert_eq!(t.product.as_deref(), Some("redis"));
        assert_eq!(t.version.as_deref(), Some("7.2"));
        assert_eq!(t.language.as_deref(), Some("C"));
    }

    #[test]
    fn detects_language_java_runtime() {
        let t = detect("eclipse-temurin:21-jre");
        assert_eq!(t.language.as_deref(), Some("Java"));
    }

    #[test]
    fn detects_language_erlang_rabbitmq() {
        let t = detect("rabbitmq:3.13-management");
        assert_eq!(t.language.as_deref(), Some("Erlang"));
    }

    #[test]
    fn detects_language_go_traefik() {
        let t = detect("traefik:v3.0");
        assert_eq!(t.language.as_deref(), Some("Go"));
    }

    #[test]
    fn detects_fluent_bit_vs_fluentd() {
        let fb = detect("fluent/fluent-bit:3.0");
        assert_eq!(fb.language.as_deref(), Some("C"));
        let fd = detect("fluent/fluentd:v1.16");
        assert_eq!(fd.language.as_deref(), Some("Ruby"));
    }

    #[test]
    fn detects_busybox() {
        let t = detect("busybox:1.36");
        assert_eq!(t.product.as_deref(), Some("busybox"));
        assert_eq!(t.language.as_deref(), Some("C"));
    }

    #[test]
    fn detects_hashicorp_vault_and_consul() {
        let vault = detect("hashicorp/vault:1.17.2");
        assert_eq!(vault.vendor.as_deref(), Some("hashicorp"));
        assert_eq!(vault.product.as_deref(), Some("vault"));
        assert_eq!(vault.language.as_deref(), Some("Go"));

        let consul = detect("hashicorp/consul:v1.20.1");
        assert_eq!(consul.vendor.as_deref(), Some("hashicorp"));
        assert_eq!(consul.product.as_deref(), Some("consul"));
        assert_eq!(consul.version.as_deref(), Some("1.20.1"));
    }

    #[test]
    fn detects_keycloak_and_zookeeper() {
        let keycloak = detect("quay.io/keycloak/keycloak:24.0.1");
        assert_eq!(keycloak.product.as_deref(), Some("keycloak"));
        assert_eq!(keycloak.language.as_deref(), Some("Java"));

        let zk = detect("bitnami/zookeeper:3.9.3");
        assert_eq!(zk.vendor.as_deref(), Some("apache"));
        assert_eq!(zk.product.as_deref(), Some("zookeeper"));
    }

    #[test]
    fn detects_observability_additions() {
        let loki = detect("grafana/loki:3.1.0");
        assert_eq!(loki.product.as_deref(), Some("loki"));
        assert_eq!(loki.language.as_deref(), Some("Go"));

        let promtail = detect("grafana/promtail:3.1.0");
        assert_eq!(promtail.product.as_deref(), Some("promtail"));

        let otel = detect("otel/opentelemetry-collector:0.104.0");
        assert_eq!(otel.vendor.as_deref(), Some("opentelemetry"));
        assert_eq!(otel.product.as_deref(), Some("otel-collector"));
    }

    #[test]
    fn detects_jaeger_components() {
        let agent = detect("jaegertracing/jaeger-agent:1.57.0");
        assert_eq!(agent.product.as_deref(), Some("jaeger-agent"));
        let collector = detect("jaegertracing/jaeger-collector:1.57.0");
        assert_eq!(collector.product.as_deref(), Some("jaeger-collector"));
        let query = detect("jaegertracing/jaeger-query:1.57.0");
        assert_eq!(query.product.as_deref(), Some("jaeger-query"));
    }

    #[test]
    fn detects_base_images() {
        let alpine = detect("alpine:3.20");
        assert_eq!(alpine.product.as_deref(), Some("alpine"));
        assert_eq!(alpine.language, None);

        let ubuntu = detect("ubuntu:24.04");
        assert_eq!(ubuntu.product.as_deref(), Some("ubuntu"));

        let distroless = detect("gcr.io/distroless/base:nonroot");
        assert_eq!(distroless.product.as_deref(), Some("distroless"));
    }

    #[test]
    fn normalizes_ubi_and_distroless_suffixes() {
        let php_ubi = detect("php:8.3-ubi9");
        assert_eq!(php_ubi.version.as_deref(), Some("8.3"));

        let node_distroless = detect("node:v22.1-distroless");
        assert_eq!(node_distroless.version.as_deref(), Some("22.1"));
    }

    #[test]
    fn detect_from_process_java() {
        let t = detect_from_process(
            &["java".to_string()],
            &["-jar".to_string(), "app.jar".to_string()],
        )
        .unwrap();
        assert_eq!(t.language.as_deref(), Some("Java"));
        assert_eq!(t.product.as_deref(), Some("java"));
        assert_eq!(t.source, "process");
    }

    #[test]
    fn detect_from_process_python_via_command() {
        let t = detect_from_process(
            &["/usr/bin/python3".to_string()],
            &["-m".to_string(), "http.server".to_string()],
        )
        .unwrap();
        assert_eq!(t.language.as_deref(), Some("Python"));
        assert_eq!(t.product.as_deref(), Some("python"));
        assert_eq!(t.source, "process");
    }

    #[test]
    fn detect_from_process_node() {
        let t = detect_from_process(&["node".to_string()], &["index.js".to_string()]).unwrap();
        assert_eq!(t.language.as_deref(), Some("JavaScript"));
        assert_eq!(t.product.as_deref(), Some("nodejs"));
    }

    #[test]
    fn detect_from_process_postgres() {
        let t = detect_from_process(&["postgres".to_string()], &[]).unwrap();
        assert_eq!(t.product.as_deref(), Some("postgres"));
        assert_eq!(t.subtype.as_deref(), Some("postgresql"));
        assert_eq!(t.source, "process");
    }

    #[test]
    fn detect_from_process_nginx() {
        let t = detect_from_process(
            &["nginx".to_string()],
            &["-g".to_string(), "daemon off;".to_string()],
        )
        .unwrap();
        assert_eq!(t.product.as_deref(), Some("nginx"));
        assert_eq!(t.source, "process");
    }

    #[test]
    fn detect_from_process_returns_none_when_command_empty() {
        let t = detect_from_process(&[], &[]);
        assert!(t.is_none());
    }

    #[test]
    fn detect_from_process_returns_none_for_unknown_executable() {
        let t = detect_from_process(&["/usr/local/bin/my-custom-app".to_string()], &[]);
        assert!(t.is_none());
    }

    // ---- Application-stack image detection ----

    #[test]
    fn detects_oracle_from_container_registry_path() {
        let t = detect_application_stack_from_image(
            "container-registry.oracle.com/database/enterprise:21.3.0.0",
        )
        .unwrap();
        assert_eq!(t.product.as_deref(), Some("oracle-database"));
        assert_eq!(t.subtype.as_deref(), Some("oracle_database"));
        assert_eq!(t.source, "image");
    }

    #[test]
    fn detects_oracle_from_gvenzl_image() {
        let t = detect_application_stack_from_image("gvenzl/oracle-xe:21-slim").unwrap();
        assert_eq!(t.subtype.as_deref(), Some("oracle_database"));
    }

    #[test]
    fn detects_spring_boot_image() {
        let t =
            detect_application_stack_from_image("myregistry/customer-springboot:1.0.0").unwrap();
        assert_eq!(t.subtype.as_deref(), Some("spring_boot"));
        assert_eq!(t.language.as_deref(), Some("Java"));
    }

    #[test]
    fn detects_angular_image() {
        let t = detect_application_stack_from_image("myregistry/angular-dashboard:1.2.3").unwrap();
        assert_eq!(t.subtype.as_deref(), Some("angular"));
    }

    #[test]
    fn detects_angular_via_dash_ng_marker() {
        let t = detect_application_stack_from_image("acme/portal-ng-prod:2.0").unwrap();
        assert_eq!(t.subtype.as_deref(), Some("angular"));
    }

    #[test]
    fn application_stack_returns_none_for_unrelated_image() {
        let t = detect_application_stack_from_image("nginx:1.25");
        assert!(t.is_none());
    }

    // ---- Spring Boot process refinement ----

    #[test]
    fn refine_spring_boot_tags_subtype_on_spring_jar() {
        let base = detect_from_process(
            &["java".to_string()],
            &[
                "-jar".to_string(),
                "customer-portal-spring-1.0.jar".to_string(),
            ],
        )
        .unwrap();
        let refined = refine_spring_boot(
            base.clone(),
            &["java".to_string()],
            &[
                "-jar".to_string(),
                "customer-portal-spring-1.0.jar".to_string(),
            ],
        );
        assert_eq!(refined.subtype.as_deref(), Some("spring_boot"));
        assert_eq!(refined.product.as_deref(), base.product.as_deref());
    }

    #[test]
    fn refine_spring_boot_tags_subtype_on_spring_profile_flag() {
        let base = detect_from_process(
            &["java".to_string()],
            &["-Dspring.profiles.active=prod".to_string()],
        )
        .unwrap();
        let refined = refine_spring_boot(
            base,
            &["java".to_string()],
            &["-Dspring.profiles.active=prod".to_string()],
        );
        assert_eq!(refined.subtype.as_deref(), Some("spring_boot"));
    }

    #[test]
    fn refine_spring_boot_is_noop_for_non_java_process() {
        let base = detect_from_process(&["node".to_string()], &["server.js".to_string()]).unwrap();
        let refined = refine_spring_boot(
            base.clone(),
            &["node".to_string()],
            &["server.js".to_string()],
        );
        assert!(refined.subtype.is_none());
    }

    // ---- Labels-based detection ----

    #[test]
    fn detect_from_labels_angular_component() {
        let labels = [("app.kubernetes.io/component", "angular")];
        let t = detect_from_labels(&labels, &[]).unwrap();
        assert_eq!(t.subtype.as_deref(), Some("angular"));
        assert_eq!(t.source, "labels");
    }

    #[test]
    fn detect_from_labels_spring_boot_component() {
        let labels = [("app.kubernetes.io/component", "spring-boot")];
        let t = detect_from_labels(&labels, &[]).unwrap();
        assert_eq!(t.subtype.as_deref(), Some("spring_boot"));
    }

    #[test]
    fn detect_from_labels_oracle_component() {
        let labels = [("app.kubernetes.io/component", "oracle")];
        let t = detect_from_labels(&labels, &[]).unwrap();
        assert_eq!(t.subtype.as_deref(), Some("oracle_database"));
    }

    #[test]
    fn detect_from_labels_postgres_component() {
        let labels = [("app.kubernetes.io/component", "postgres")];
        let t = detect_from_labels(&labels, &[]).unwrap();
        assert_eq!(t.product.as_deref(), Some("postgres"));
        assert_eq!(t.subtype.as_deref(), Some("postgresql"));
        assert_eq!(t.source, "labels");
    }

    #[test]
    fn detect_from_labels_postgresql_component() {
        let labels = [("app.kubernetes.io/component", "postgresql")];
        let t = detect_from_labels(&labels, &[]).unwrap();
        assert_eq!(t.product.as_deref(), Some("postgres"));
        assert_eq!(t.subtype.as_deref(), Some("postgresql"));
    }

    #[test]
    fn detect_from_labels_angular_via_annotation() {
        let annotations = [("angular.io/version", "17.0.0")];
        let t = detect_from_labels(&[], &annotations).unwrap();
        assert_eq!(t.subtype.as_deref(), Some("angular"));
        assert_eq!(t.version.as_deref(), Some("17.0.0"));
    }

    #[test]
    fn detect_from_labels_spring_via_annotation() {
        let annotations = [("app.spring.io/version", "3.2.0")];
        let t = detect_from_labels(&[], &annotations).unwrap();
        assert_eq!(t.subtype.as_deref(), Some("spring_boot"));
        assert_eq!(t.version.as_deref(), Some("3.2.0"));
    }

    #[test]
    fn detect_from_labels_returns_none_when_no_markers() {
        let labels = [("unrelated", "value")];
        let t = detect_from_labels(&labels, &[]);
        assert!(t.is_none());
    }
}
