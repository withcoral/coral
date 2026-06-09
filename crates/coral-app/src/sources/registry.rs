//! Client for the hosted community source registry.

use std::fmt::Write as _;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::sources::model::{CommunitySourceProvenance, SourceUpdateInfo};

#[derive(Clone)]
pub(crate) struct CommunitySourceRegistry {
    base_url: String,
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub(crate) struct CommunitySourceCatalog {
    pub(crate) repository: RegistryRepository,
    pub(crate) sources: Vec<RegistrySourceEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RegistryRepository {
    pub(crate) full_name: String,
    #[serde(rename = "ref")]
    pub(crate) git_ref: String,
    pub(crate) commit_sha: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RegistrySourceEntry {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) version: String,
    pub(crate) paths: RegistrySourcePaths,
    pub(crate) hashes: RegistrySourceHashes,
    pub(crate) git: RegistrySourceGit,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RegistrySourcePaths {
    pub(crate) manifest: String,
    #[serde(default)]
    pub(crate) readme: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RegistrySourceHashes {
    pub(crate) manifest_sha256: String,
    #[serde(default)]
    pub(crate) readme_sha256: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RegistrySourceGit {
    #[serde(rename = "commit_sha")]
    pub(crate) commit: String,
    #[serde(default)]
    #[serde(rename = "manifest_blob_sha")]
    pub(crate) manifest_blob: Option<String>,
    #[serde(default)]
    #[serde(rename = "readme_blob_sha")]
    pub(crate) readme_blob: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RegistryIndexResponse {
    repository: RegistryRepository,
    sources: Vec<RegistrySourceEntry>,
}

impl CommunitySourceRegistry {
    pub(crate) fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub(crate) async fn discover_sources(&self) -> Result<CommunitySourceCatalog, AppError> {
        let response = self
            .client
            .get(format!("{}/v1/community/sources", self.base_url))
            .send()
            .await
            .map_err(registry_error)?;
        if !response.status().is_success() {
            return Err(registry_status_error(response.status().as_u16()));
        }
        let index: RegistryIndexResponse = response.json().await.map_err(registry_error)?;
        Ok(CommunitySourceCatalog {
            repository: index.repository,
            sources: index.sources,
        })
    }

    pub(crate) async fn get_source(
        &self,
        source_name: &SourceName,
    ) -> Result<(RegistryRepository, RegistrySourceEntry), AppError> {
        let catalog = self.discover_sources().await?;
        let entry = catalog
            .sources
            .into_iter()
            .find(|source| source.name == source_name.as_str())
            .ok_or_else(|| AppError::SourceNotFound(source_name.to_string()))?;
        Ok((catalog.repository, entry))
    }

    pub(crate) async fn get_manifest(&self, source_name: &SourceName) -> Result<String, AppError> {
        let response = self
            .client
            .get(format!(
                "{}/v1/community/sources/{}/manifest",
                self.base_url,
                source_name.as_str()
            ))
            .send()
            .await
            .map_err(registry_error)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(AppError::SourceNotFound(source_name.to_string()));
        }
        if !response.status().is_success() {
            return Err(registry_status_error(response.status().as_u16()));
        }
        response.text().await.map_err(registry_error)
    }
}

impl RegistrySourceEntry {
    pub(crate) fn provenance(&self, repository: &RegistryRepository) -> CommunitySourceProvenance {
        CommunitySourceProvenance {
            repository: repository.full_name.clone(),
            git_ref: repository.git_ref.clone(),
            commit_sha: if self.git.commit.is_empty() {
                repository.commit_sha.clone()
            } else {
                self.git.commit.clone()
            },
            manifest_path: self.paths.manifest.clone(),
            manifest_sha256: self.hashes.manifest_sha256.clone(),
            readme_path: self.paths.readme.clone(),
            readme_sha256: self.hashes.readme_sha256.clone(),
            manifest_blob_sha: self.git.manifest_blob.clone(),
            readme_blob_sha: self.git.readme_blob.clone(),
        }
    }

    pub(crate) fn update_info(
        &self,
        installed_version: &str,
        installed: &CommunitySourceProvenance,
    ) -> SourceUpdateInfo {
        let update_available = installed.manifest_sha256 != self.hashes.manifest_sha256
            || installed.commit_sha != self.git.commit;
        SourceUpdateInfo {
            update_available,
            installed_version: installed_version.to_string(),
            latest_version: self.version.clone(),
            installed_manifest_sha256: installed.manifest_sha256.clone(),
            latest_manifest_sha256: self.hashes.manifest_sha256.clone(),
            latest_commit_sha: self.git.commit.clone(),
        }
    }
}

pub(crate) fn sha256_hex(raw: &str) -> String {
    let digest = Sha256::digest(raw.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn registry_error(error: reqwest::Error) -> AppError {
    let error = error.without_url();
    AppError::FailedPrecondition(format!("community source registry request failed: {error}"))
}

fn registry_status_error(status: u16) -> AppError {
    AppError::FailedPrecondition(format!("community source registry returned HTTP {status}"))
}
