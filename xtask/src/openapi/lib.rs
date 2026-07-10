//! `OpenAPI` reference hydration for the `openapi-hydrate` xtask subcommand.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use serde::Deserialize;
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use thiserror::Error;
use url::Url;

/// Maximum number of bytes accepted for the root `OpenAPI` descriptor.
pub(crate) const ROOT_DESCRIPTOR_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// Maximum number of bytes accepted for a referenced external descriptor.
pub(crate) const EXTERNAL_REF_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// Timeout for HTTPS fetches.
pub(crate) const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// User-Agent sent for HTTPS fetches.
pub(crate) const USER_AGENT: &str = "openapi";

/// Maximum number of external documents fetched at the same time.
pub(crate) const MAX_CONCURRENT_FETCHES: usize = 32;

/// Errors returned by `OpenAPI` hydration.
#[derive(Debug, Error)]
pub(crate) enum OpenApiToolsError {
    /// The input location or base URI is invalid.
    #[error("invalid location or base URI `{location}`: {message}")]
    InvalidLocation {
        /// Location or base URI that could not be used.
        location: String,
        /// Human-readable detail.
        message: String,
    },

    /// The URI scheme is not supported.
    #[error("unsupported URI scheme `{scheme}` for `{location}`")]
    UnsupportedScheme {
        /// Unsupported scheme.
        scheme: String,
        /// Location that used the scheme.
        location: String,
    },

    /// A local file could not be read.
    #[error("failed to read `{location}`: {source}")]
    ReadFailure {
        /// Location that could not be read.
        location: String,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// A network fetch failed.
    #[error("failed to fetch `{location}`: {source}")]
    FetchFailure {
        /// URL that could not be fetched.
        location: String,
        /// Underlying HTTP client error.
        #[source]
        source: reqwest::Error,
    },

    /// An HTTPS request redirected to a non-HTTPS URL.
    #[error("HTTPS request for `{location}` redirected to non-HTTPS `{redirected_to}`")]
    RedirectToNonHttps {
        /// Original URL.
        location: String,
        /// Final redirected URL.
        redirected_to: String,
    },

    /// The HTTP response status was not successful.
    #[error("`{location}` returned unsuccessful HTTP status {status}")]
    ResponseNotSuccessful {
        /// URL that returned the status.
        location: String,
        /// HTTP status code.
        status: reqwest::StatusCode,
    },

    /// A descriptor exceeded the configured maximum size.
    #[error("`{location}` exceeded size limit of {limit} bytes")]
    SizeLimitExceeded {
        /// Location that exceeded the limit.
        location: String,
        /// Maximum permitted bytes.
        limit: u64,
    },

    /// A descriptor could not be parsed as YAML or JSON.
    #[error("failed to parse `{location}` as YAML or JSON: {source}")]
    ParseFailure {
        /// Location that failed to parse.
        location: String,
        /// Parser error.
        #[source]
        source: serde_yaml::Error,
    },

    /// A reachable JSON Pointer target was missing.
    #[error("unresolved ref target `{reference}` in `{base}`")]
    UnresolvedRefTarget {
        /// Document containing the ref.
        base: String,
        /// Ref that could not be resolved.
        reference: String,
    },

    /// A fragment syntax is not supported.
    #[error("unsupported fragment `{fragment}` in `{reference}`")]
    UnsupportedFragment {
        /// Ref containing the unsupported fragment.
        reference: String,
        /// Unsupported fragment.
        fragment: String,
    },

    /// A local file reference would escape the root descriptor directory.
    #[error("local file ref `{reference}` resolves outside descriptor directory `{root}`")]
    LocalFileConfinementViolation {
        /// Ref that escaped confinement.
        reference: String,
        /// Root descriptor directory.
        root: PathBuf,
    },

    /// Final dereferencing failed.
    #[error("failed to dereference OpenAPI document: {message}")]
    DereferenceFailure {
        /// Human-readable dereference error.
        message: String,
    },
}

#[derive(Debug, Clone)]
struct Document {
    value: Value,
}

#[derive(Debug)]
struct Resolver {
    root_uri: String,
    root_dir: Option<PathBuf>,
    client: reqwest::blocking::Client,
    document_cache: HashMap<String, Document>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct RefTarget {
    doc_uri: String,
    fragment: String,
}

/// Hydrates an `OpenAPI` document from bytes and a base URI.
///
/// # Errors
///
/// Returns [`OpenApiToolsError`] if the descriptor cannot be parsed, if a
/// reachable reference cannot be loaded or resolved, or if final dereferencing
/// fails.
pub(crate) fn hydrate_openapi(input: &[u8], base_uri: &str) -> Result<Value, OpenApiToolsError> {
    let root_uri = normalize_base_uri(base_uri)?;
    let document_url = parse_document_url(&root_uri)?;
    let root_dir = root_directory_from_url(&document_url)?;
    let root = parse_document(input, &root_uri)?;
    let mut resolver = Resolver::new(root_uri.clone(), root_dir)?;
    resolver
        .document_cache
        .insert(root_uri.clone(), Document { value: root });
    resolver.hydrate()
}

/// Hydrates an `OpenAPI` document from an HTTPS URL or local file path.
///
/// # Errors
///
/// Returns [`OpenApiToolsError`] if the location cannot be read, fetched,
/// parsed, or dereferenced.
pub(crate) fn hydrate_openapi_from_location(location: &str) -> Result<Value, OpenApiToolsError> {
    if let Ok(mut url) = Url::parse(location) {
        return match url.scheme() {
            "https" => {
                url.set_fragment(None);
                let client = http_client()?;
                let bytes = fetch_https(&client, url.as_str(), ROOT_DESCRIPTOR_MAX_BYTES)?;
                hydrate_openapi(&bytes, location)
            }
            "file" => {
                let url = strip_query_and_fragment(url);
                let path = url
                    .to_file_path()
                    .map_err(|()| OpenApiToolsError::InvalidLocation {
                        location: location.to_owned(),
                        message: "file URL cannot be converted to a local path".to_owned(),
                    })?;
                hydrate_file_path(&path)
            }
            scheme => Err(OpenApiToolsError::UnsupportedScheme {
                scheme: scheme.to_owned(),
                location: location.to_owned(),
            }),
        };
    }

    hydrate_file_path(Path::new(location))
}

impl Resolver {
    fn new(root_uri: String, root_dir: Option<PathBuf>) -> Result<Self, OpenApiToolsError> {
        Ok(Self {
            root_uri,
            root_dir,
            client: http_client()?,
            document_cache: HashMap::new(),
        })
    }

    fn hydrate(&mut self) -> Result<Value, OpenApiToolsError> {
        let mut queue = VecDeque::new();
        let mut reachable_root_components = BTreeSet::new();
        let mut seen_refs = BTreeSet::new();

        let root_value = &self
            .document_cache
            .get(&self.root_uri)
            .expect("root document inserted before hydration")
            .value;
        collect_refs_from_root_document(
            root_value,
            &self.root_uri,
            &self.root_uri,
            &mut queue,
            &mut reachable_root_components,
        )?;

        while !queue.is_empty() {
            let mut pending_targets = Vec::new();
            let mut missing_doc_uris = BTreeSet::new();
            while let Some(target) = queue.pop_front() {
                if !seen_refs.insert(target.clone()) {
                    continue;
                }
                if !self.document_cache.contains_key(&target.doc_uri) {
                    missing_doc_uris.insert(target.doc_uri.clone());
                }
                pending_targets.push(target);
            }

            self.load_missing_documents(missing_doc_uris)?;

            for target in pending_targets {
                let value = self
                    .document_cache
                    .get(&target.doc_uri)
                    .and_then(|doc| pointer_target(&doc.value, &target.fragment))
                    .ok_or_else(|| OpenApiToolsError::UnresolvedRefTarget {
                        base: target.doc_uri.clone(),
                        reference: format!("{}#{}", target.doc_uri, target.fragment),
                    })?
                    .clone();
                collect_refs_in_value(
                    &value,
                    &target.doc_uri,
                    &self.root_uri,
                    &mut queue,
                    &mut reachable_root_components,
                )?;
            }
        }

        let mut pruned_root = {
            let root = &self
                .document_cache
                .get(&self.root_uri)
                .expect("root document inserted before hydration")
                .value;
            prune_root_components(root, &reachable_root_components)
        };
        normalize_same_document_refs(&mut pruned_root, &self.root_uri)?;

        let resources = self
            .document_cache
            .iter()
            .filter(|(uri, _doc)| uri.as_str() != self.root_uri)
            .map(|(uri, doc)| (uri.as_str(), doc.value.clone()))
            .collect::<Vec<_>>();
        let mut registry = jsonschema::Registry::new();
        for (uri, value) in &resources {
            registry = registry.add(*uri, value).map_err(|error| {
                OpenApiToolsError::DereferenceFailure {
                    message: error.to_string(),
                }
            })?;
        }
        let registry =
            registry
                .prepare()
                .map_err(|error| OpenApiToolsError::DereferenceFailure {
                    message: error.to_string(),
                })?;

        jsonschema::options()
            .with_registry(&registry)
            .with_base_uri(self.root_uri.clone())
            .dereference(&pruned_root)
            .map_err(|error| OpenApiToolsError::DereferenceFailure {
                message: error.to_string(),
            })
    }

    fn load_missing_documents(
        &mut self,
        doc_uris: BTreeSet<String>,
    ) -> Result<(), OpenApiToolsError> {
        let missing_doc_uris = doc_uris
            .into_iter()
            .filter(|doc_uri| !self.document_cache.contains_key(doc_uri))
            .collect::<Vec<_>>();

        for chunk in missing_doc_uris.chunks(MAX_CONCURRENT_FETCHES) {
            let mut handles = Vec::new();
            for doc_uri in chunk {
                let client = self.client.clone();
                let root_dir = self.root_dir.clone();
                let doc_uri = doc_uri.clone();
                handles.push(thread::spawn(move || {
                    load_external_document(&client, root_dir.as_deref(), &doc_uri)
                        .map(|value| (doc_uri, value))
                }));
            }

            for handle in handles {
                let (doc_uri, value) =
                    handle
                        .join()
                        .map_err(|_error| OpenApiToolsError::DereferenceFailure {
                            message: "external ref loader thread panicked".to_owned(),
                        })??;
                self.document_cache
                    .entry(doc_uri)
                    .or_insert_with(|| Document { value });
            }
        }

        Ok(())
    }
}

fn load_external_document(
    client: &reqwest::blocking::Client,
    root_dir: Option<&Path>,
    doc_uri: &str,
) -> Result<Value, OpenApiToolsError> {
    let url = parse_document_url(doc_uri)?;
    let bytes = match url.scheme() {
        "https" => fetch_https(client, doc_uri, EXTERNAL_REF_MAX_BYTES)?,
        "file" => read_confined_file_ref(root_dir, &url, doc_uri)?,
        scheme => {
            return Err(OpenApiToolsError::UnsupportedScheme {
                scheme: scheme.to_owned(),
                location: doc_uri.to_owned(),
            });
        }
    };
    parse_document(&bytes, doc_uri)
}

fn read_confined_file_ref(
    root_dir: Option<&Path>,
    url: &Url,
    reference: &str,
) -> Result<Vec<u8>, OpenApiToolsError> {
    let root = root_dir.ok_or_else(|| OpenApiToolsError::UnsupportedScheme {
        scheme: "file".to_owned(),
        location: reference.to_owned(),
    })?;
    let path = url
        .to_file_path()
        .map_err(|()| OpenApiToolsError::InvalidLocation {
            location: reference.to_owned(),
            message: "file URL cannot be converted to a local path".to_owned(),
        })?;
    let canonical = path
        .canonicalize()
        .map_err(|source| OpenApiToolsError::ReadFailure {
            location: reference.to_owned(),
            source,
        })?;
    if !canonical.starts_with(root) {
        return Err(OpenApiToolsError::LocalFileConfinementViolation {
            reference: reference.to_owned(),
            root: root.to_path_buf(),
        });
    }
    reject_symlink_target(&path, reference)?;
    read_limited_file(&path, EXTERNAL_REF_MAX_BYTES, reference)
}

#[cfg(test)]
fn cache_miss_doc_uris<'a>(
    targets: impl IntoIterator<Item = &'a RefTarget>,
    document_cache: &HashMap<String, Document>,
) -> BTreeSet<String> {
    targets
        .into_iter()
        .filter(|target| !document_cache.contains_key(&target.doc_uri))
        .map(|target| target.doc_uri.clone())
        .collect()
}

fn hydrate_file_path(path: &Path) -> Result<Value, OpenApiToolsError> {
    let bytes = read_limited_file(path, ROOT_DESCRIPTOR_MAX_BYTES, &path.display().to_string())?;
    let canonical = path
        .canonicalize()
        .map_err(|source| OpenApiToolsError::ReadFailure {
            location: path.display().to_string(),
            source,
        })?;
    reject_symlink_target(path, &path.display().to_string())?;
    let url = Url::from_file_path(&canonical).map_err(|()| OpenApiToolsError::InvalidLocation {
        location: path.display().to_string(),
        message: "file path cannot be represented as a file URL".to_owned(),
    })?;
    hydrate_openapi(&bytes, url.as_str())
}

fn http_client() -> Result<reqwest::blocking::Client, OpenApiToolsError> {
    reqwest::blocking::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .user_agent(USER_AGENT)
        .build()
        .map_err(|source| OpenApiToolsError::FetchFailure {
            location: "https client".to_owned(),
            source,
        })
}

fn read_limited_file(
    path: &Path,
    limit: u64,
    location: &str,
) -> Result<Vec<u8>, OpenApiToolsError> {
    if let Ok(metadata) = fs::metadata(path)
        && metadata.len() > limit
    {
        return Err(OpenApiToolsError::SizeLimitExceeded {
            location: location.to_owned(),
            limit,
        });
    }
    let file = File::open(path).map_err(|source| OpenApiToolsError::ReadFailure {
        location: location.to_owned(),
        source,
    })?;
    read_limited(file, limit, location)
}

fn read_limited(
    reader: impl io::Read,
    limit: u64,
    location: &str,
) -> Result<Vec<u8>, OpenApiToolsError> {
    let mut limited = reader.take(limit.saturating_add(1));
    let mut bytes = Vec::new();
    limited
        .read_to_end(&mut bytes)
        .map_err(|source| OpenApiToolsError::ReadFailure {
            location: location.to_owned(),
            source,
        })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limit {
        return Err(OpenApiToolsError::SizeLimitExceeded {
            location: location.to_owned(),
            limit,
        });
    }
    Ok(bytes)
}

fn fetch_https(
    client: &reqwest::blocking::Client,
    location: &str,
    limit: u64,
) -> Result<Vec<u8>, OpenApiToolsError> {
    let response =
        client
            .get(location)
            .send()
            .map_err(|source| OpenApiToolsError::FetchFailure {
                location: location.to_owned(),
                source,
            })?;
    if response.url().scheme() != "https" {
        return Err(OpenApiToolsError::RedirectToNonHttps {
            location: location.to_owned(),
            redirected_to: response.url().to_string(),
        });
    }
    let status = response.status();
    if !status.is_success() {
        return Err(OpenApiToolsError::ResponseNotSuccessful {
            location: location.to_owned(),
            status,
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(OpenApiToolsError::SizeLimitExceeded {
            location: location.to_owned(),
            limit,
        });
    }
    read_limited(response, limit, location)
}

fn parse_document(input: &[u8], location: &str) -> Result<Value, OpenApiToolsError> {
    let value = serde_yaml::from_slice::<JsonValue>(input).map_err(|source| {
        OpenApiToolsError::ParseFailure {
            location: location.to_owned(),
            source,
        }
    })?;
    Ok(value.0)
}

struct JsonValue(Value);

// Deserialize through our own visitor because `serde_json::Value` rejects YAML
// integers outside its native i64/u64 range before callers can handle them.
impl<'de> Deserialize<'de> for JsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JsonValueVisitor)
    }
}

struct JsonValueVisitor;

impl<'de> Visitor<'de> for JsonValueVisitor {
    type Value = JsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a YAML value that can be represented as JSON")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::Number(Number::from(value))))
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if let Ok(value) = i64::try_from(value) {
            Ok(JsonValue(Value::Number(Number::from(value))))
        } else {
            f64_json_number(i128_to_f64_lossy(value)).map(|number| JsonValue(Value::Number(number)))
        }
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::Number(Number::from(value))))
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if let Ok(value) = u64::try_from(value) {
            Ok(JsonValue(Value::Number(Number::from(value))))
        } else {
            f64_json_number(u128_to_f64_lossy(value)).map(|number| JsonValue(Value::Number(number)))
        }
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        f64_json_number(value).map(|number| JsonValue(Value::Number(number)))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(JsonValue(Value::Null))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element::<JsonValue>()? {
            values.push(value.0);
        }
        Ok(JsonValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut object = Map::new();
        while let Some(key) = map.next_key_seed(JsonMapKeySeed)? {
            let value = map.next_value::<JsonValue>()?;
            object.insert(key, value.0);
        }
        Ok(JsonValue(Value::Object(object)))
    }
}

struct JsonMapKeySeed;

impl<'de> DeserializeSeed<'de> for JsonMapKeySeed {
    type Value = String;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JsonMapKeyVisitor)
    }
}

struct JsonMapKeyVisitor;

impl<'de> Visitor<'de> for JsonMapKeyVisitor {
    type Value = String;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a scalar YAML mapping key")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(value.to_string())
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(value.to_string())
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E> {
        Ok(value.to_string())
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(value.to_string())
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E> {
        Ok(value.to_string())
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let number = f64_json_number(value)?;
        Ok(number.to_string())
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(value.to_owned())
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(value)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok("null".to_owned())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok("null".to_owned())
    }

    fn visit_seq<A>(self, _seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        Err(de::Error::custom(
            "YAML sequence keys cannot be represented as JSON object keys",
        ))
    }

    fn visit_map<A>(self, _map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        Err(de::Error::custom(
            "YAML mapping keys cannot be represented as JSON object keys",
        ))
    }
}

fn f64_json_number<E>(value: f64) -> Result<Number, E>
where
    E: de::Error,
{
    Number::from_f64(value).ok_or_else(|| {
        de::Error::custom("YAML number is not representable as a finite JSON number")
    })
}

#[expect(
    clippy::cast_precision_loss,
    reason = "serde_json::Number cannot represent i128; large YAML integers are preserved as finite JSON numbers with f64 precision."
)]
fn i128_to_f64_lossy(value: i128) -> f64 {
    value as f64
}

#[expect(
    clippy::cast_precision_loss,
    reason = "serde_json::Number cannot represent u128; large YAML integers are preserved as finite JSON numbers with f64 precision."
)]
fn u128_to_f64_lossy(value: u128) -> f64 {
    value as f64
}

fn normalize_base_uri(base_uri: &str) -> Result<String, OpenApiToolsError> {
    let url = Url::parse(base_uri).map_err(|error| OpenApiToolsError::InvalidLocation {
        location: base_uri.to_owned(),
        message: error.to_string(),
    })?;
    Ok(strip_query_and_fragment(url).to_string())
}

fn strip_query_and_fragment(mut url: Url) -> Url {
    url.set_query(None);
    url.set_fragment(None);
    url
}

fn parse_document_url(location: &str) -> Result<Url, OpenApiToolsError> {
    Url::parse(location).map_err(|error| OpenApiToolsError::InvalidLocation {
        location: location.to_owned(),
        message: error.to_string(),
    })
}

fn root_directory_from_url(url: &Url) -> Result<Option<PathBuf>, OpenApiToolsError> {
    if url.scheme() != "file" {
        return Ok(None);
    }
    let path = url
        .to_file_path()
        .map_err(|()| OpenApiToolsError::InvalidLocation {
            location: url.to_string(),
            message: "file URL cannot be converted to a local path".to_owned(),
        })?;
    let parent = path
        .parent()
        .ok_or_else(|| OpenApiToolsError::InvalidLocation {
            location: url.to_string(),
            message: "file descriptor has no parent directory".to_owned(),
        })?;
    parent
        .canonicalize()
        .map(Some)
        .map_err(|source| OpenApiToolsError::ReadFailure {
            location: parent.display().to_string(),
            source,
        })
}

fn reject_symlink_target(path: &Path, location: &str) -> Result<(), OpenApiToolsError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| OpenApiToolsError::ReadFailure {
        location: location.to_owned(),
        source,
    })?;
    if metadata.file_type().is_symlink() {
        return Err(OpenApiToolsError::LocalFileConfinementViolation {
            reference: location.to_owned(),
            root: path
                .parent()
                .map_or_else(|| PathBuf::from("."), Path::to_path_buf),
        });
    }
    Ok(())
}

fn collect_refs_from_root_document(
    root: &Value,
    base_uri: &str,
    root_uri: &str,
    queue: &mut VecDeque<RefTarget>,
    reachable_root_components: &mut BTreeSet<Vec<String>>,
) -> Result<(), OpenApiToolsError> {
    let Value::Object(object) = root else {
        return collect_refs_in_value(root, base_uri, root_uri, queue, reachable_root_components);
    };

    for (key, child) in object {
        if key != "components" {
            collect_refs_in_value(child, base_uri, root_uri, queue, reachable_root_components)?;
        }
    }
    Ok(())
}

fn collect_refs_in_value(
    value: &Value,
    base_uri: &str,
    root_uri: &str,
    queue: &mut VecDeque<RefTarget>,
    reachable_root_components: &mut BTreeSet<Vec<String>>,
) -> Result<(), OpenApiToolsError> {
    match value {
        Value::Object(object) => {
            if let Some(Value::String(reference)) = object.get("$ref") {
                let target = resolve_ref(base_uri, reference)?;
                if target.doc_uri == root_uri
                    && let Some(component_path) = root_component_path(&target.fragment)
                {
                    reachable_root_components.insert(component_path);
                }
                queue.push_back(target);
            }
            for child in object.values() {
                collect_refs_in_value(child, base_uri, root_uri, queue, reachable_root_components)?;
            }
        }
        Value::Array(array) => {
            for child in array {
                collect_refs_in_value(child, base_uri, root_uri, queue, reachable_root_components)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn resolve_ref(base_uri: &str, reference: &str) -> Result<RefTarget, OpenApiToolsError> {
    let base = parse_document_url(base_uri)?;
    let joined = base
        .join(reference)
        .map_err(|error| OpenApiToolsError::InvalidLocation {
            location: reference.to_owned(),
            message: error.to_string(),
        })?;
    let scheme = joined.scheme();
    if scheme != "https" && scheme != "file" {
        return Err(OpenApiToolsError::UnsupportedScheme {
            scheme: scheme.to_owned(),
            location: reference.to_owned(),
        });
    }
    if base.scheme() == "https" && scheme == "file" {
        return Err(OpenApiToolsError::UnsupportedScheme {
            scheme: "file".to_owned(),
            location: reference.to_owned(),
        });
    }
    let fragment = joined.fragment().unwrap_or_default().to_owned();
    validate_fragment(reference, &fragment)?;
    let doc_uri = strip_query_and_fragment(joined).to_string();
    Ok(RefTarget { doc_uri, fragment })
}

fn validate_fragment(reference: &str, fragment: &str) -> Result<(), OpenApiToolsError> {
    let decoded =
        urlencoding::decode(fragment).map_err(|error| OpenApiToolsError::UnsupportedFragment {
            reference: reference.to_owned(),
            fragment: format!("{fragment}: {error}"),
        })?;
    if decoded.is_empty() || decoded.starts_with('/') {
        Ok(())
    } else {
        Err(OpenApiToolsError::UnsupportedFragment {
            reference: reference.to_owned(),
            fragment: fragment.to_owned(),
        })
    }
}

fn pointer_target<'a>(value: &'a Value, fragment: &str) -> Option<&'a Value> {
    if fragment.is_empty() {
        return Some(value);
    }
    let decoded = urlencoding::decode(fragment).ok()?;
    value.pointer(decoded.as_ref())
}

fn root_component_path(fragment: &str) -> Option<Vec<String>> {
    let decoded = urlencoding::decode(fragment).ok()?;
    let tokens = pointer_tokens(decoded.as_ref())?;
    if tokens.len() >= 3 && tokens.first().is_some_and(|token| token == "components") {
        Some(tokens.into_iter().take(3).collect())
    } else {
        None
    }
}

fn pointer_tokens(pointer: &str) -> Option<Vec<String>> {
    if pointer.is_empty() {
        return Some(Vec::new());
    }
    pointer
        .strip_prefix('/')
        .map(|stripped| stripped.split('/').map(unescape_pointer_token).collect())
}

fn unescape_pointer_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

fn normalize_same_document_refs(value: &mut Value, doc_uri: &str) -> Result<(), OpenApiToolsError> {
    match value {
        Value::Object(object) => {
            if let Some(Value::String(reference)) = object.get_mut("$ref") {
                let target = resolve_ref(doc_uri, reference)?;
                if target.doc_uri == doc_uri {
                    *reference = format!("#{}", target.fragment);
                }
            }
            for child in object.values_mut() {
                normalize_same_document_refs(child, doc_uri)?;
            }
        }
        Value::Array(array) => {
            for child in array {
                normalize_same_document_refs(child, doc_uri)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn prune_root_components(root: &Value, reachable_components: &BTreeSet<Vec<String>>) -> Value {
    let mut pruned = root.clone();
    let Some(components) = pruned.get_mut("components").and_then(Value::as_object_mut) else {
        return pruned;
    };

    let mut next_components = Map::new();
    for path in reachable_components {
        let [_, group, name] = path.as_slice() else {
            continue;
        };
        let Some(value) = components
            .get(group)
            .and_then(|group_value| group_value.get(name))
        else {
            continue;
        };
        next_components
            .entry(group.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        if let Some(group_object) = next_components
            .get_mut(group)
            .and_then(Value::as_object_mut)
        {
            group_object.insert(name.clone(), value.clone());
        }
    }
    *components = next_components;
    pruned
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_miss_doc_uris_deduplicates_fragments_by_resolved_uri() {
        let targets = [
            RefTarget {
                doc_uri: "https://example.com/schemas.yaml".to_owned(),
                fragment: "/Pet".to_owned(),
            },
            RefTarget {
                doc_uri: "https://example.com/schemas.yaml".to_owned(),
                fragment: "/Category".to_owned(),
            },
        ];
        let cache = HashMap::new();

        let missing = cache_miss_doc_uris(targets.iter(), &cache);

        assert_eq!(
            missing,
            BTreeSet::from(["https://example.com/schemas.yaml".to_owned()])
        );
    }

    #[test]
    fn cache_miss_doc_uris_skips_cached_documents() {
        let targets = [RefTarget {
            doc_uri: "https://example.com/schemas.yaml".to_owned(),
            fragment: "/Pet".to_owned(),
        }];
        let cache = HashMap::from([(
            "https://example.com/schemas.yaml".to_owned(),
            Document { value: Value::Null },
        )]);

        let missing = cache_miss_doc_uris(targets.iter(), &cache);

        assert!(missing.is_empty());
    }

    #[test]
    fn hydrate_accepts_yaml_integer_above_u64_max_as_json_number() {
        let input = br##"
openapi: 3.1.0
info: {title: Test, version: "1"}
paths:
  /mysql:
    get:
      responses:
        "200":
          description: ok
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Config"
components:
  schemas:
    Config:
      type: object
      properties:
        group_concat_max_len:
          type: integer
          minimum: 4
          maximum: 18446744073709552000
"##;

        let hydrated =
            hydrate_openapi(input, "https://example.com/openapi.yaml").expect("hydrates");

        let maximum = hydrated
            .pointer(
                "/paths/~1mysql/get/responses/200/content/application~1json/schema/properties/group_concat_max_len/maximum",
            )
            .and_then(Value::as_f64)
            .expect("maximum remains a JSON number");
        assert!((maximum - 18_446_744_073_709_552_000.0).abs() < f64::EPSILON);
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod integration_tests;
