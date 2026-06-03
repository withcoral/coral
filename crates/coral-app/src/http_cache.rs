//! `[http_cache]` config section: tunables for the HTTP response cache.

use std::collections::HashMap;

use serde::Deserialize;

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;

/// Default per-source cache capacity when no override is supplied.
const DEFAULT_PER_SOURCE_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// `[http_cache]` settings loaded from `config.toml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HttpCacheConfig {
    pub(super) enabled: bool,
    pub(super) default_max_bytes_per_source: u64,
    pub(super) total_max_bytes: Option<u64>,
    pub(super) per_source_max_bytes: HashMap<String, u64>,
}

impl Default for HttpCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_max_bytes_per_source: DEFAULT_PER_SOURCE_MAX_BYTES,
            total_max_bytes: None,
            per_source_max_bytes: HashMap::new(),
        }
    }
}

impl HttpCacheConfig {
    /// Load the `[http_cache]` section from `config.toml`. Returns defaults
    /// when the file or section is missing.
    pub(super) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        if !layout.config_file().exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(layout.config_file())?;
        let file = toml::from_str::<HttpCacheConfigFile>(&raw)?;
        file.http_cache.try_into()
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct HttpCacheConfigFile {
    #[serde(default)]
    http_cache: RawHttpCacheConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawHttpCacheConfig {
    enabled: bool,
    default_max_bytes_per_source: RawByteSize,
    total_max_bytes: Option<RawByteSize>,
    sources: HashMap<String, RawSourceCacheConfig>,
}

impl Default for RawHttpCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_max_bytes_per_source: RawByteSize::Integer(DEFAULT_PER_SOURCE_MAX_BYTES),
            total_max_bytes: None,
            sources: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSourceCacheConfig {
    max_bytes: RawByteSize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawByteSize {
    Integer(u64),
    String(String),
}

impl RawByteSize {
    fn parse(&self) -> Result<u64, String> {
        match self {
            Self::Integer(value) => Ok(*value),
            Self::String(value) => parse_byte_size(value),
        }
    }
}

impl TryFrom<RawHttpCacheConfig> for HttpCacheConfig {
    type Error = AppError;

    fn try_from(raw: RawHttpCacheConfig) -> Result<Self, Self::Error> {
        let default_max_bytes_per_source = raw
            .default_max_bytes_per_source
            .parse()
            .map_err(|err| http_cache_error("default_max_bytes_per_source", &err))?;
        let total_max_bytes = raw
            .total_max_bytes
            .as_ref()
            .map(RawByteSize::parse)
            .transpose()
            .map_err(|err| http_cache_error("total_max_bytes", &err))?;
        let mut per_source_max_bytes = HashMap::with_capacity(raw.sources.len());
        for (source_name, source_config) in raw.sources {
            let bytes = source_config.max_bytes.parse().map_err(|err| {
                http_cache_error(&format!("sources.{source_name}.max_bytes"), &err)
            })?;
            per_source_max_bytes.insert(source_name, bytes);
        }
        Ok(Self {
            enabled: raw.enabled,
            default_max_bytes_per_source,
            total_max_bytes,
            per_source_max_bytes,
        })
    }
}

fn http_cache_error(field: &str, detail: &str) -> AppError {
    AppError::InvalidInput(format!("[http_cache].{field}: {detail}"))
}

/// Parse a human-readable byte size string. Accepts decimal suffixes (KB/MB/GB,
/// 1000-based) and binary suffixes (KiB/MiB/GiB, 1024-based). Bare integers and
/// the `B` suffix are interpreted as bytes. Case-insensitive on the suffix.
fn parse_byte_size(value: &str) -> Result<u64, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("must not be empty".to_string());
    }
    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, suffix) = trimmed.split_at(split_at);
    if digits.is_empty() {
        return Err(format!("'{value}' missing a numeric value"));
    }
    let number: u64 = digits
        .parse()
        .map_err(|error| format!("'{value}' is not a valid byte count: {error}"))?;
    let multiplier: u64 = match suffix.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "KIB" => 1 << 10,
        "MIB" => 1 << 20,
        "GIB" => 1 << 30,
        other => {
            return Err(format!(
                "'{value}' has unknown unit '{other}'; use B, KB, MB, GB, KiB, MiB, or GiB"
            ));
        }
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("'{value}' overflows u64 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_byte_size_accepts_plain_integers_as_bytes() {
        assert_eq!(parse_byte_size("1024").unwrap(), 1024);
        assert_eq!(parse_byte_size("0").unwrap(), 0);
    }

    #[test]
    fn parse_byte_size_handles_binary_and_decimal_suffixes() {
        assert_eq!(parse_byte_size("1KiB").unwrap(), 1024);
        assert_eq!(parse_byte_size("1MiB").unwrap(), 1024 * 1024);
        assert_eq!(parse_byte_size("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("1KB").unwrap(), 1000);
        assert_eq!(parse_byte_size("1MB").unwrap(), 1_000_000);
        assert_eq!(parse_byte_size("1GB").unwrap(), 1_000_000_000);
    }

    #[test]
    fn parse_byte_size_is_case_insensitive() {
        assert_eq!(parse_byte_size("256mib").unwrap(), 256 * 1024 * 1024);
        assert_eq!(parse_byte_size("256 MiB").unwrap(), 256 * 1024 * 1024);
    }

    #[test]
    fn parse_byte_size_rejects_unknown_suffix() {
        parse_byte_size("10TB").expect_err("unknown suffix should fail");
        parse_byte_size("ten").expect_err("missing digits should fail");
        parse_byte_size("").expect_err("empty value should fail");
    }

    #[test]
    fn parse_byte_size_rejects_overflow() {
        parse_byte_size("18446744073709551615GiB").expect_err("overflow should fail");
    }

    fn load_with_config(toml: &str) -> Result<HttpCacheConfig, AppError> {
        use std::io::Write;
        let temp = tempfile::TempDir::new()?;
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))?;
        layout.ensure()?;
        let mut file = std::fs::File::create(layout.config_file())?;
        file.write_all(toml.as_bytes())?;
        HttpCacheConfig::load(&layout)
    }

    #[test]
    fn defaults_when_section_is_missing() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let cfg = HttpCacheConfig::load(&layout).expect("default");
        assert_eq!(cfg, HttpCacheConfig::default());
        assert!(cfg.enabled);
        assert_eq!(
            cfg.default_max_bytes_per_source,
            DEFAULT_PER_SOURCE_MAX_BYTES
        );
        assert_eq!(cfg.total_max_bytes, None);
        assert!(cfg.per_source_max_bytes.is_empty());
    }

    #[test]
    fn loads_per_source_overrides_and_total_ceiling() {
        let cfg = load_with_config(
            r#"
[http_cache]
default_max_bytes_per_source = "128MiB"
total_max_bytes  = "2GiB"

[http_cache.sources.github]
max_bytes = "500MiB"

[http_cache.sources.jsonph]
max_bytes = "16MiB"
"#,
        )
        .expect("config loads");
        assert!(cfg.enabled);
        assert_eq!(cfg.default_max_bytes_per_source, 128 * 1024 * 1024);
        assert_eq!(cfg.total_max_bytes, Some(2 * 1024 * 1024 * 1024));
        assert_eq!(
            cfg.per_source_max_bytes.get("github"),
            Some(&(500 * 1024 * 1024))
        );
        assert_eq!(
            cfg.per_source_max_bytes.get("jsonph"),
            Some(&(16 * 1024 * 1024))
        );
    }

    #[test]
    fn loads_bare_integer_byte_limits() {
        let cfg = load_with_config(
            r"
[http_cache]
default_max_bytes_per_source = 134217728
total_max_bytes  = 2147483648

[http_cache.sources.github]
max_bytes = 524288000
",
        )
        .expect("config loads");
        assert_eq!(cfg.default_max_bytes_per_source, 128 * 1024 * 1024);
        assert_eq!(cfg.total_max_bytes, Some(2 * 1024 * 1024 * 1024));
        assert_eq!(
            cfg.per_source_max_bytes.get("github"),
            Some(&(500 * 1024 * 1024))
        );
    }

    #[test]
    fn enabled_false_disables_cache() {
        let cfg = load_with_config(
            r"
[http_cache]
enabled = false
",
        )
        .expect("config loads");
        assert!(!cfg.enabled);
    }

    #[test]
    fn invalid_byte_string_surfaces_field_path() {
        let err = load_with_config(
            r#"
[http_cache]
default_max_bytes_per_source = "many"
"#,
        )
        .expect_err("invalid bytes should fail");
        let message = format!("{err}");
        assert!(
            message.contains("[http_cache].default_max_bytes_per_source"),
            "expected error to mention field path; got: {message}"
        );
    }
}
