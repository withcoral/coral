//! Host-local record of what this host has reconciled into its config mirror.
//!
//! The ledger lives next to `config.toml` as `config.toml.ledger` and is read
//! and written only under the state lock, always *after* the write it records.
//! Old binaries never read it: to them it is an unknown sibling file.
//!
//! A crash between a mirror write and its ledger update fails toward the safe
//! direction by construction. An unrecorded or mismatched record looks
//! operator-authored, which means resurrect-and-import for tombstone and update
//! decisions and set-aside-and-warn for manifest and divergence decisions —
//! never a silent overwrite of content this host cannot vouch for.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "the reconciliation passes wire this next")
)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::bootstrap::AppError;
use crate::hash::sha256_hex;
use crate::sources::SourceName;
use crate::sources::model::InstalledSource;
use crate::storage::fs as storage_fs;
use crate::workspaces::WorkspaceName;

/// Ledger schema version. A file written by any other version is discarded
/// rather than interpreted: a wrong record claims a reconciliation this host
/// never performed, and an empty ledger only costs a re-import.
const LEDGER_VERSION: u32 = 1;

/// What this host last reconciled for one source, per artifact.
///
/// Every field is independently optional because the three writes happen on
/// different paths and at different times.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[expect(
    clippy::struct_field_names,
    reason = "each field names the artifact its hash covers"
)]
pub(crate) struct LedgerEntry {
    /// Hash of the canonical serialization of the [`InstalledSource`] this host
    /// last reconciled into its `config.toml`. `None` means this host has never
    /// reconciled the entry.
    pub(crate) entry_sha256: Option<String>,
    /// Hash of the `manifest.yaml` bytes Coral last wrote to this host's file.
    pub(crate) manifest_sha256: Option<String>,
    /// Warn-once bookkeeping for an unproven divergent config entry: the hash
    /// this host last warned about. Never treated as reconciled — it only
    /// suppresses repeating one warning every boot.
    pub(crate) divergence_warned_sha256: Option<String>,
}

/// The host-local ledger for one config directory.
#[derive(Debug, Clone, Default)]
pub(crate) struct MirrorLedger {
    workspaces: BTreeSet<WorkspaceName>,
    sources: BTreeMap<(WorkspaceName, SourceName), LedgerEntry>,
}

impl MirrorLedger {
    /// Read the ledger beside `config_path`.
    ///
    /// A missing, unreadable, corrupt, or foreign-version file loads as empty.
    /// This never fails: the ledger is an optimization over the safe default,
    /// so losing it must never keep the server from booting.
    pub(crate) fn load(config_path: &Path) -> Self {
        let path = ledger_path(config_path);
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(error) => {
                if error.kind() != ErrorKind::NotFound {
                    warn!(%error, path = %path.display(), "ignoring unreadable mirror ledger");
                }
                return Self::default();
            }
        };
        match serde_json::from_slice::<PersistedLedger>(&raw) {
            Ok(persisted) if persisted.version == LEDGER_VERSION => Self::from_persisted(persisted),
            Ok(persisted) => {
                warn!(
                    version = persisted.version,
                    path = %path.display(),
                    "ignoring mirror ledger written by another schema version"
                );
                Self::default()
            }
            Err(error) => {
                warn!(%error, path = %path.display(), "ignoring corrupt mirror ledger");
                Self::default()
            }
        }
    }

    /// Write the ledger beside `config_path`, atomically.
    pub(crate) fn save(&self, config_path: &Path) -> Result<(), AppError> {
        let path = ledger_path(config_path);
        if let Some(parent) = path.parent() {
            storage_fs::ensure_dir(parent)?;
        }
        let raw = serde_json::to_vec_pretty(&self.to_persisted())?;
        storage_fs::write_atomic(&path, &raw)?;
        Ok(())
    }

    /// Record that this host has reconciled `workspace` with the database.
    pub(crate) fn record_workspace(&mut self, workspace: &WorkspaceName) {
        self.workspaces.insert(workspace.clone());
    }

    /// Whether this host has reconciled `workspace`.
    pub(crate) fn has_workspace(&self, workspace: &WorkspaceName) -> bool {
        self.workspaces.contains(workspace)
    }

    /// Forget `workspace` and every source record it owns, on workspace delete.
    pub(crate) fn remove_workspace(&mut self, workspace: &WorkspaceName) {
        self.workspaces.remove(workspace);
        self.sources
            .retain(|(entry_workspace, _), _| entry_workspace != workspace);
    }

    /// Record the config entry this host just mirrored for `name`.
    pub(crate) fn record_entry(
        &mut self,
        workspace: &WorkspaceName,
        name: &SourceName,
        source: &InstalledSource,
    ) {
        if let Some(sha256) = canonical_entry_sha256(source) {
            self.entry_mut(workspace, name).entry_sha256 = Some(sha256);
        }
    }

    /// Record the `manifest.yaml` bytes this host just wrote for `name`.
    pub(crate) fn record_manifest(
        &mut self,
        workspace: &WorkspaceName,
        name: &SourceName,
        sha256: &str,
    ) {
        self.entry_mut(workspace, name).manifest_sha256 = Some(sha256.to_string());
    }

    /// Record that this host already warned about divergent content `sha256`.
    pub(crate) fn record_divergence_warned(
        &mut self,
        workspace: &WorkspaceName,
        name: &SourceName,
        sha256: &str,
    ) {
        self.entry_mut(workspace, name).divergence_warned_sha256 = Some(sha256.to_string());
    }

    /// Forget every record for `name`, on source delete.
    pub(crate) fn remove(&mut self, workspace: &WorkspaceName, name: &SourceName) {
        self.sources.remove(&(workspace.clone(), name.clone()));
    }

    /// Whether `source` is byte-for-byte the entry this host last mirrored.
    pub(crate) fn matches_entry(
        &self,
        workspace: &WorkspaceName,
        name: &SourceName,
        source: &InstalledSource,
    ) -> bool {
        let Some(recorded) = self.entry_sha256(workspace, name) else {
            return false;
        };
        canonical_entry_sha256(source).is_some_and(|current| current == recorded)
    }

    /// Whether this host has ever reconciled the config entry for `name`.
    ///
    /// Keyed on the entry hash alone, so a divergence warning — which records
    /// content this host explicitly did *not* vouch for — can never answer yes.
    pub(crate) fn entry_recorded(&self, workspace: &WorkspaceName, name: &SourceName) -> bool {
        self.entry_sha256(workspace, name).is_some()
    }

    /// Whether `sha256` is the manifest content this host last wrote.
    pub(crate) fn matches_manifest(
        &self,
        workspace: &WorkspaceName,
        name: &SourceName,
        sha256: &str,
    ) -> bool {
        self.entry(workspace, name)
            .and_then(|entry| entry.manifest_sha256.as_deref())
            == Some(sha256)
    }

    fn entry(&self, workspace: &WorkspaceName, name: &SourceName) -> Option<&LedgerEntry> {
        self.sources.get(&(workspace.clone(), name.clone()))
    }

    fn entry_sha256(&self, workspace: &WorkspaceName, name: &SourceName) -> Option<&str> {
        self.entry(workspace, name)
            .and_then(|entry| entry.entry_sha256.as_deref())
    }

    fn entry_mut(&mut self, workspace: &WorkspaceName, name: &SourceName) -> &mut LedgerEntry {
        self.sources
            .entry((workspace.clone(), name.clone()))
            .or_default()
    }

    fn to_persisted(&self) -> PersistedLedger {
        PersistedLedger {
            version: LEDGER_VERSION,
            workspaces: self
                .workspaces
                .iter()
                .map(|workspace| workspace.as_str().to_string())
                .collect(),
            sources: self
                .sources
                .iter()
                .map(|((workspace, name), entry)| PersistedEntry {
                    workspace: workspace.as_str().to_string(),
                    source: name.as_str().to_string(),
                    entry_sha256: entry.entry_sha256.clone(),
                    manifest_sha256: entry.manifest_sha256.clone(),
                    divergence_warned_sha256: entry.divergence_warned_sha256.clone(),
                })
                .collect(),
        }
    }

    fn from_persisted(persisted: PersistedLedger) -> Self {
        Self {
            workspaces: persisted
                .workspaces
                .iter()
                .filter_map(|workspace| WorkspaceName::parse(workspace).ok())
                .collect(),
            sources: persisted
                .sources
                .into_iter()
                .filter_map(|entry| {
                    let workspace = WorkspaceName::parse(&entry.workspace).ok()?;
                    let name = SourceName::parse(&entry.source).ok()?;
                    Some((
                        (workspace, name),
                        LedgerEntry {
                            entry_sha256: entry.entry_sha256,
                            manifest_sha256: entry.manifest_sha256,
                            divergence_warned_sha256: entry.divergence_warned_sha256,
                        },
                    ))
                })
                .collect(),
        }
    }
}

/// `config.toml` -> `config.toml.ledger`.
fn ledger_path(config_path: &Path) -> PathBuf {
    let mut path = config_path.as_os_str().to_os_string();
    path.push(".ledger");
    PathBuf::from(path)
}

/// Hash the canonical serialization of `source` rather than its rendered
/// `config.toml` bytes: the mirror re-renders the whole catalog, so formatting
/// normalization must never make a Coral-written entry look operator-authored.
///
/// `None` means the entry could not be canonicalized, which the callers treat
/// as "not recorded" — never as a match.
fn canonical_entry_sha256(source: &InstalledSource) -> Option<String> {
    match serde_json::to_vec(source) {
        Ok(canonical) => Some(sha256_hex(&canonical)),
        Err(error) => {
            warn!(%error, source = %source.name, "could not canonicalize a source for the mirror ledger");
            None
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedLedger {
    version: u32,
    #[serde(default)]
    workspaces: Vec<String>,
    #[serde(default)]
    sources: Vec<PersistedEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedEntry {
    workspace: String,
    source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entry_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    manifest_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    divergence_warned_sha256: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::TempDir;
    use uuid::Uuid;

    use super::{LEDGER_VERSION, MirrorLedger, ledger_path};
    use crate::credentials::CredentialStorageKind;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::workspaces::WorkspaceName;

    fn config_path(dir: &TempDir) -> std::path::PathBuf {
        dir.path().join("config.toml")
    }

    fn workspace(name: &str) -> WorkspaceName {
        WorkspaceName::parse(name).expect("parse workspace name")
    }

    fn source_name(name: &str) -> SourceName {
        SourceName::parse(name).expect("parse source name")
    }

    fn installed(name: &str) -> InstalledSource {
        InstalledSource {
            name: source_name(name),
            version: Some("1.2.3".to_string()),
            variables: BTreeMap::from([
                ("owner".to_string(), "withcoral".to_string()),
                ("repo".to_string(), "coral".to_string()),
            ]),
            secrets: vec!["token".to_string()],
            credential_storage: Some(CredentialStorageKind::File),
            credential_revision: Uuid::nil(),
            origin: SourceOrigin::Imported,
        }
    }

    #[test]
    fn ledger_is_a_sibling_of_the_config_file() {
        let dir = TempDir::new().expect("temp dir");
        assert_eq!(
            ledger_path(&config_path(&dir)),
            dir.path().join("config.toml.ledger")
        );
    }

    #[test]
    fn a_missing_ledger_loads_as_empty() {
        let dir = TempDir::new().expect("temp dir");
        let ledger = MirrorLedger::load(&config_path(&dir));

        assert!(!ledger.has_workspace(&workspace("team")));
        assert!(!ledger.entry_recorded(&workspace("team"), &source_name("github")));
    }

    #[test]
    fn a_corrupt_ledger_loads_as_empty() {
        let dir = TempDir::new().expect("temp dir");
        let config = config_path(&dir);
        std::fs::write(ledger_path(&config), b"{ not json at all").expect("write corrupt ledger");

        let ledger = MirrorLedger::load(&config);

        assert!(!ledger.has_workspace(&workspace("team")));
        assert!(!ledger.entry_recorded(&workspace("team"), &source_name("github")));
    }

    /// A ledger from another schema version is discarded rather than read:
    /// a record we cannot interpret must never claim a reconciliation.
    #[test]
    fn a_foreign_version_ledger_loads_as_empty() {
        let dir = TempDir::new().expect("temp dir");
        let config = config_path(&dir);
        let raw = format!(
            r#"{{"version":{},"workspaces":["team"],"sources":[]}}"#,
            LEDGER_VERSION + 1
        );
        std::fs::write(ledger_path(&config), raw).expect("write foreign ledger");

        assert!(!MirrorLedger::load(&config).has_workspace(&workspace("team")));
    }

    /// Names that no longer parse are dropped instead of failing the load.
    #[test]
    fn unparsable_names_are_skipped_on_load() {
        let dir = TempDir::new().expect("temp dir");
        let config = config_path(&dir);
        let raw = format!(
            r#"{{"version":{LEDGER_VERSION},"workspaces":["../escape"],"sources":[{{"workspace":"team","source":"../escape","entry_sha256":"abc"}}]}}"#
        );
        std::fs::write(ledger_path(&config), raw).expect("write ledger");

        let ledger = MirrorLedger::load(&config);

        assert!(!ledger.has_workspace(&workspace("team")));
        assert!(!ledger.entry_recorded(&workspace("team"), &source_name("github")));
    }

    #[test]
    fn save_and_load_round_trip_workspaces_and_every_hash() {
        let dir = TempDir::new().expect("temp dir");
        let config = config_path(&dir);
        let team = workspace("team");
        let github = source_name("github");
        let source = installed("github");

        let mut ledger = MirrorLedger::default();
        ledger.record_workspace(&team);
        ledger.record_entry(&team, &github, &source);
        ledger.record_manifest(&team, &github, "manifest-hash");
        ledger.record_divergence_warned(&team, &github, "warned-hash");
        ledger.save(&config).expect("save ledger");

        let loaded = MirrorLedger::load(&config);

        assert!(loaded.has_workspace(&team));
        assert!(loaded.entry_recorded(&team, &github));
        assert!(loaded.matches_entry(&team, &github, &source));
        assert!(loaded.matches_manifest(&team, &github, "manifest-hash"));
        assert_eq!(
            loaded
                .entry(&team, &github)
                .and_then(|entry| entry.divergence_warned_sha256.as_deref()),
            Some("warned-hash")
        );
    }

    /// `write_atomic` renames a temp file into place, so a completed save
    /// leaves the ledger and nothing else behind.
    #[test]
    fn save_leaves_no_partial_file_behind() {
        let dir = TempDir::new().expect("temp dir");
        let config = config_path(&dir);
        let mut ledger = MirrorLedger::default();
        ledger.record_workspace(&workspace("team"));
        ledger.save(&config).expect("save ledger");

        let written: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read temp dir")
            .map(|entry| entry.expect("dir entry").file_name())
            .collect();

        assert_eq!(written, vec!["config.toml.ledger"]);
    }

    /// The record means "this exact content was reconciled here"; a warn-once
    /// note records content this host explicitly did not vouch for.
    #[test]
    fn a_divergence_warning_alone_never_reads_as_reconciled() {
        let team = workspace("team");
        let github = source_name("github");
        let source = installed("github");

        let mut ledger = MirrorLedger::default();
        ledger.record_divergence_warned(&team, &github, "warned-hash");

        assert!(!ledger.entry_recorded(&team, &github));
        assert!(!ledger.matches_entry(&team, &github, &source));
        assert!(!ledger.matches_manifest(&team, &github, "warned-hash"));
    }

    #[test]
    fn a_changed_entry_no_longer_matches_but_stays_recorded() {
        let team = workspace("team");
        let github = source_name("github");
        let mut source = installed("github");

        let mut ledger = MirrorLedger::default();
        ledger.record_entry(&team, &github, &source);
        assert!(ledger.matches_entry(&team, &github, &source));

        source.version = Some("9.9.9".to_string());

        assert!(!ledger.matches_entry(&team, &github, &source));
        assert!(ledger.entry_recorded(&team, &github));
    }

    /// The hash covers the canonical value, not rendered file bytes, so an
    /// entry rebuilt in a different order still matches.
    #[test]
    fn entry_hashing_is_insensitive_to_construction_order() {
        let team = workspace("team");
        let github = source_name("github");
        let mut source = installed("github");

        let mut ledger = MirrorLedger::default();
        ledger.record_entry(&team, &github, &source);

        source.variables = BTreeMap::from([
            ("repo".to_string(), "coral".to_string()),
            ("owner".to_string(), "withcoral".to_string()),
        ]);

        assert!(ledger.matches_entry(&team, &github, &source));
    }

    #[test]
    fn matches_manifest_is_keyed_on_the_recorded_bytes() {
        let team = workspace("team");
        let github = source_name("github");

        let mut ledger = MirrorLedger::default();
        ledger.record_manifest(&team, &github, "manifest-hash");

        assert!(ledger.matches_manifest(&team, &github, "manifest-hash"));
        assert!(!ledger.matches_manifest(&team, &github, "other-hash"));
        assert!(!ledger.matches_manifest(&team, &source_name("linear"), "manifest-hash"));
    }

    #[test]
    fn remove_drops_one_sources_records() {
        let team = workspace("team");
        let github = source_name("github");
        let linear = source_name("linear");

        let mut ledger = MirrorLedger::default();
        ledger.record_entry(&team, &github, &installed("github"));
        ledger.record_entry(&team, &linear, &installed("linear"));
        ledger.remove(&team, &github);

        assert!(!ledger.entry_recorded(&team, &github));
        assert!(ledger.entry_recorded(&team, &linear));
    }

    #[test]
    fn remove_workspace_drops_the_workspace_and_its_source_entries() {
        let team = workspace("team");
        let other = workspace("other");
        let github = source_name("github");

        let mut ledger = MirrorLedger::default();
        ledger.record_workspace(&team);
        ledger.record_workspace(&other);
        ledger.record_entry(&team, &github, &installed("github"));
        ledger.record_manifest(&team, &github, "manifest-hash");
        ledger.record_entry(&other, &github, &installed("github"));

        ledger.remove_workspace(&team);

        assert!(!ledger.has_workspace(&team));
        assert!(!ledger.entry_recorded(&team, &github));
        assert!(!ledger.matches_manifest(&team, &github, "manifest-hash"));
        assert!(ledger.has_workspace(&other));
        assert!(ledger.entry_recorded(&other, &github));
    }
}
