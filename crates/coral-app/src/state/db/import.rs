use std::collections::{BTreeMap, BTreeSet};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use chrono::Utc;
use tracing::{info, warn};

use super::session::DbRepos;
use super::{CoralDb, MaterializationRecord, SourceManifestRecord, now_unix_nanos_i64};
use crate::bootstrap::AppError;
use crate::hash::sha256_hex;
use crate::sources::SourceName;
use crate::sources::catalog::resolve_installed_manifest;
use crate::sources::materialization::{
    hydrate_v4_materialization_cache, read_v4_materialization_record,
};
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::mirror_ledger::MirrorLedger;
use crate::state::{AppConfig, AppStateLayout, ConfigStore};
use crate::storage::fs as storage_fs;
use crate::workspaces::{WorkspaceName, WorkspaceRecord};

const WORKSPACE_CATALOG_CUTOVER_ID: &str = "workspace_catalog_cutover_v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceCatalogCutoverReport {
    pub(crate) workspace_count: usize,
    pub(crate) cutover_performed: bool,
}

pub(crate) async fn run_state_migrations(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<(), AppError> {
    cutover_legacy_workspace_catalog(db, config_store, layout).await?;
    reconcile_source_state(db, config_store, layout)
        .await?
        .log();
    remove_legacy_task_jsonl(config_store, layout)?;
    Ok(())
}

/// What one boot's reconciliation did: one counter per branch of the
/// discrimination rule, then what the mirror and hydration passes rebuilt from
/// the rows.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct SourceImportReport {
    pub(crate) imported: usize,
    /// Present row the ledger proves the entry still matches.
    pub(crate) already_present: usize,
    /// Present row whose entry the ledger proves was rewritten locally, so the
    /// file content won and was imported as an update.
    pub(crate) updated_from_files: usize,
    /// No ledger record and an entry that disagrees with the row: warned,
    /// preserved on both sides, never stamped as reconciled.
    pub(crate) divergent_unreconciled: usize,
    /// Warn-and-skip for legacy data that could not be read, never a startup
    /// failure.
    pub(crate) skipped_invalid: usize,
    /// A stale mirror of a source another host deleted through the database.
    pub(crate) skipped_tombstoned: usize,
    /// A tombstoned name whose entry changed since this host reconciled it —
    /// a genuine re-add, which clears the tombstone.
    pub(crate) readded_after_tombstone: usize,
    /// Config-only workspaces given a row so their sources stay visible.
    pub(crate) workspaces_created: usize,
    /// Config-only workspaces the ledger proves were deleted through the
    /// database, skipped together with their sources.
    pub(crate) workspaces_skipped_deleted: usize,
    /// Manifest and materialization caches rebuilt from their rows.
    pub(crate) hydrated_caches: usize,
    /// Mirror reconciliation: database rows added to the config.
    pub(crate) mirrored_entries: usize,
    /// Mirror reconciliation: ledger-proven stale config entries rewritten
    /// from the database.
    pub(crate) mirror_entries_refreshed: usize,
}

impl SourceImportReport {
    fn log(&self) {
        info!(
            imported = self.imported,
            already_present = self.already_present,
            updated_from_files = self.updated_from_files,
            divergent_unreconciled = self.divergent_unreconciled,
            skipped_invalid = self.skipped_invalid,
            skipped_tombstoned = self.skipped_tombstoned,
            readded_after_tombstone = self.readded_after_tombstone,
            workspaces_created = self.workspaces_created,
            workspaces_skipped_deleted = self.workspaces_skipped_deleted,
            hydrated_caches = self.hydrated_caches,
            mirrored_entries = self.mirrored_entries,
            mirror_entries_refreshed = self.mirror_entries_refreshed,
            "reconciled the legacy source catalog with the database"
        );
    }
}

/// Reconciles this host's file world with the database, in three passes.
///
/// The import carries the config file's source catalog into the database, the
/// mirror pass carries the database's rows back into `config.toml` where this
/// host's copy is provably behind, and hydration rebuilds the artifact caches
/// the rows are the record for. All three run under one hold of the state lock
/// and over one ledger, because each pass rules on what the previous one
/// recorded.
///
/// Runs on every boot and carries no marker: a source — or a whole workspace —
/// an older binary added to `config.toml` after the last boot has to be picked
/// up at the next one, and a marker is exactly what would stop that.
///
/// Additive by construction. It never removes a database row and never removes
/// a config entry; unreadable legacy data is warned about and skipped, so only
/// a broken database fails startup.
async fn reconcile_source_state(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<SourceImportReport, AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    let config_path = layout.config_file();
    let mut import = SourceImport {
        db,
        layout,
        ledger: MirrorLedger::load(config_path),
        report: SourceImportReport::default(),
        now_unix_nanos: now_unix_nanos_i64()?,
    };
    let config = match config_store.load_config_unlocked() {
        Ok(config) => config,
        Err(error) => {
            warn!("skipping the legacy source import: config.toml could not be read: {error}");
            return Ok(import.report);
        }
    };

    let workspaces = import.reconcilable_workspaces(&config).await?;
    for workspace in &workspaces {
        import.import_workspace_sources(&config, workspace).await?;
    }
    for workspace in &workspaces {
        import
            .reconcile_source_mirror(config_store, &config, workspace)
            .await?;
        import.hydrate_missing_artifact_caches(workspace).await?;
    }

    if let Err(error) = import.ledger.save(config_path) {
        warn!("source reconciliation finished, but the mirror ledger could not be saved: {error}");
    }
    Ok(import.report)
}

/// One boot's import, carrying the state every branch of the discrimination
/// rule reads and writes.
struct SourceImport<'a> {
    db: &'a CoralDb,
    layout: &'a AppStateLayout,
    ledger: MirrorLedger,
    report: SourceImportReport,
    now_unix_nanos: i64,
}

impl SourceImport<'_> {
    /// The workspaces whose sources this boot imports: the database's rows,
    /// plus the config-only workspaces the ledger does not prove were deleted.
    ///
    /// The union is what carries a workspace an older binary created across —
    /// the workspace cutover is one-shot, so nothing else would — and the
    /// ledger gate is what stops it from resurrecting a workspace another host
    /// deleted through the database, boot after boot.
    async fn reconcilable_workspaces(
        &mut self,
        config: &AppConfig,
    ) -> Result<Vec<WorkspaceName>, AppError> {
        let mut session = self.db;
        let rows = session.workspaces().list().await?;
        let mut workspaces = Vec::with_capacity(rows.len());
        for row in rows {
            let Ok(name) = WorkspaceName::parse(&row.id) else {
                continue;
            };
            self.ledger.record_workspace(&name);
            workspaces.push(name);
        }

        let known = workspaces.iter().cloned().collect::<BTreeSet<_>>();
        for record in config.legacy_workspace_records() {
            if known.contains(&record.name) {
                continue;
            }
            if self.ledger.has_workspace(&record.name) {
                warn!(
                    "skipping workspace '{}' and its sources: this host reconciled it and the database no longer holds it, so it was deleted through the database",
                    record.name
                );
                self.report.workspaces_skipped_deleted += 1;
                continue;
            }
            let mut tx = self.db.begin().await?;
            tx.workspaces()
                .ensure(record.name.as_str(), self.now_unix_nanos)
                .await?;
            tx.commit().await?;
            self.ledger.record_workspace(&record.name);
            self.report.workspaces_created += 1;
            workspaces.push(record.name);
        }
        Ok(workspaces)
    }

    async fn import_workspace_sources(
        &mut self,
        config: &AppConfig,
        workspace: &WorkspaceName,
    ) -> Result<(), AppError> {
        let entries = config.workspace_sources(workspace);
        if entries.is_empty() {
            return Ok(());
        }
        let mut session = self.db;
        let rows = session
            .sources()
            .list_workspace_sources(workspace)
            .await?
            .into_iter()
            .map(|source| (source.name.clone(), source))
            .collect::<BTreeMap<_, _>>();
        for entry in entries {
            match rows.get(&entry.name) {
                Some(row) => {
                    self.reconcile_present_source(workspace, &entry, row)
                        .await?;
                }
                None => self.import_absent_source(workspace, &entry).await?,
            }
        }
        Ok(())
    }

    /// The three-way rule for an entry that already has a row.
    ///
    /// A ledger match is provably this host's own mirror of the row and needs
    /// nothing. A ledger record the entry no longer matches means the entry was
    /// rewritten here since — by a downgraded binary or by an operator — and
    /// the file content wins. Without a record, agreement seeds the ledger and
    /// disagreement is unprovable, so neither side is touched.
    async fn reconcile_present_source(
        &mut self,
        workspace: &WorkspaceName,
        entry: &InstalledSource,
        row: &InstalledSource,
    ) -> Result<(), AppError> {
        if self.ledger.matches_entry(workspace, &entry.name, entry) {
            self.report.already_present += 1;
            return self.seed_manifest_record(workspace, entry).await;
        }
        if self.ledger.entry_recorded(workspace, &entry.name) {
            if self.import_source(workspace, entry).await? {
                self.report.updated_from_files += 1;
            } else {
                self.report.skipped_invalid += 1;
            }
            return Ok(());
        }
        if entry == row {
            self.report.already_present += 1;
            self.ledger.record_entry(workspace, &entry.name, entry);
            return self.seed_manifest_record(workspace, entry).await;
        }

        self.report.divergent_unreconciled += 1;
        if !self
            .ledger
            .matches_divergence_warning(workspace, &entry.name, entry)
        {
            warn!(
                "the config entry for source '{}' in workspace '{}' disagrees with its database row and this host has never reconciled it; leaving both untouched — re-import the source to make the file win, or fix the entry",
                entry.name, workspace
            );
            self.ledger
                .record_divergence_warned(workspace, &entry.name, entry);
        }
        Ok(())
    }

    /// The rule for an entry with no row: a tombstone the ledger still matches
    /// is another host's deletion reaching this host's stale mirror, and
    /// anything else is content to import — a re-add if the name was deleted.
    async fn import_absent_source(
        &mut self,
        workspace: &WorkspaceName,
        entry: &InstalledSource,
    ) -> Result<(), AppError> {
        let mut session = self.db;
        let tombstoned = session
            .sources()
            .is_tombstoned(workspace, &entry.name)
            .await?;
        if tombstoned && self.ledger.matches_entry(workspace, &entry.name, entry) {
            warn!(
                "skipping source '{}' in workspace '{}': its config entry is this host's stale mirror of a source deleted through the database",
                entry.name, workspace
            );
            self.report.skipped_tombstoned += 1;
            return Ok(());
        }
        if !self.import_source(workspace, entry).await? {
            self.report.skipped_invalid += 1;
        } else if tombstoned {
            self.report.readded_after_tombstone += 1;
        } else {
            self.report.imported += 1;
        }
        Ok(())
    }

    /// Writes one source and its artifacts in a transaction of their own, so
    /// one unreadable source never blocks the rest, and records what landed.
    ///
    /// Reports whether the source was imported; `false` means it was warned
    /// about and skipped.
    async fn import_source(
        &mut self,
        workspace: &WorkspaceName,
        entry: &InstalledSource,
    ) -> Result<bool, AppError> {
        let materialization = self.read_v4_materialization(workspace, &entry.name);
        let mut tx = self.db.begin().await?;
        tx.sources()
            .upsert_source(workspace, entry, self.now_unix_nanos)
            .await?;
        let manifest_yaml = match entry.origin {
            // A bundled source's manifest ships with the binary, so there is
            // nothing on disk that is worth storing.
            SourceOrigin::Bundled => None,
            SourceOrigin::Imported => {
                match resolve_installed_manifest(workspace, entry, self.layout) {
                    Ok(manifest) => Some(manifest.manifest_yaml),
                    Err(error) => {
                        tx.rollback().await?;
                        warn!(
                            "skipping source '{}' in workspace '{}': its manifest could not be read: {error}",
                            entry.name, workspace
                        );
                        return Ok(false);
                    }
                }
            }
        };
        match manifest_yaml.as_deref() {
            Some(manifest_yaml) => {
                tx.source_manifests()
                    .upsert(workspace, &entry.name, manifest_yaml, self.now_unix_nanos)
                    .await?;
            }
            None => {
                tx.source_manifests().remove(workspace, &entry.name).await?;
            }
        }
        match materialization.as_ref() {
            Some(materialization) => {
                tx.materializations()
                    .upsert(workspace, &entry.name, materialization, self.now_unix_nanos)
                    .await?;
            }
            None => {
                tx.materializations().remove(workspace, &entry.name).await?;
            }
        }
        tx.commit().await?;

        self.ledger.record_entry(workspace, &entry.name, entry);
        if let Some(manifest_yaml) = manifest_yaml {
            self.ledger.record_manifest(
                workspace,
                &entry.name,
                &sha256_hex(manifest_yaml.as_bytes()),
            );
        }
        Ok(true)
    }

    /// Records an on-disk manifest the row proves is Coral's own.
    ///
    /// Without this, a host's first reconciled boot would leave every cache
    /// unproven, and the hydration pass would set aside caches that are in fact
    /// byte-identical to the artifact of record.
    async fn seed_manifest_record(
        &mut self,
        workspace: &WorkspaceName,
        entry: &InstalledSource,
    ) -> Result<(), AppError> {
        if entry.origin != SourceOrigin::Imported {
            return Ok(());
        }
        let Ok(bytes) = std::fs::read(self.layout.manifest_file(workspace, &entry.name)) else {
            return Ok(());
        };
        let sha256 = sha256_hex(&bytes);
        if self
            .ledger
            .matches_manifest(workspace, &entry.name, &sha256)
        {
            return Ok(());
        }
        let mut session = self.db;
        if session
            .source_manifests()
            .get(workspace, &entry.name)
            .await?
            .is_some_and(|record| record.manifest_hash == sha256)
        {
            self.ledger.record_manifest(workspace, &entry.name, &sha256);
        }
        Ok(())
    }

    /// Reads an installed `materialized/v4` directory back as the row that
    /// reproduces it, or nothing if it is absent or incomplete.
    ///
    /// A directory that is missing a required artifact is warned about and left
    /// out, exactly as a partially written one is today — the next install
    /// rebuilds it. Legacy state never fails a boot.
    fn read_v4_materialization(
        &self,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) -> Option<MaterializationRecord> {
        let materialized_dir = self.layout.v4_materialized_dir(workspace, source_name);
        match read_v4_materialization_record(&materialized_dir) {
            Ok(record) => record,
            Err(error) => {
                warn!(
                    "importing source '{source_name}' in workspace '{workspace}' without its materialization: {error}"
                );
                None
            }
        }
    }

    /// Brings the config mirror back up to the rows it mirrors.
    ///
    /// A row with no entry is the mirror write a crash separated from its
    /// commit — or another host's addition — and is written out. An entry that
    /// disagrees with its row while still matching this host's ledger is
    /// provably Coral's own stale copy of a row another host has since updated,
    /// and is rewritten; that is what keeps a downgraded binary's catalog
    /// current for updates rather than only for additions. Anything else
    /// disagrees with the ledger too, so it is an operator's entry and is left
    /// byte-for-byte alone — the import pass has already warned about it, and
    /// the served catalog is the database either way.
    ///
    /// Entries are never removed: a stale entry of a source deleted through
    /// another host stays visible to a downgraded binary and inert to this one.
    async fn reconcile_source_mirror(
        &mut self,
        config_store: &ConfigStore,
        config: &AppConfig,
        workspace: &WorkspaceName,
    ) -> Result<(), AppError> {
        let entries = config
            .workspace_sources(workspace)
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect::<BTreeMap<_, _>>();
        let mut session = self.db;
        for row in session.sources().list_workspace_sources(workspace).await? {
            match entries.get(&row.name) {
                None => {
                    if self.write_mirror_entry(config_store, workspace, &row) {
                        self.report.mirrored_entries += 1;
                    }
                }
                Some(entry)
                    if *entry != row && self.ledger.matches_entry(workspace, &row.name, entry) =>
                {
                    if self.write_mirror_entry(config_store, workspace, &row) {
                        self.report.mirror_entries_refreshed += 1;
                    }
                }
                Some(_) => {}
            }
        }
        Ok(())
    }

    /// Writes one row into the config mirror and records it, reporting whether
    /// it landed.
    ///
    /// A mirror this host cannot write is warned about rather than fatal: the
    /// database is what this binary serves, and the next boot tries again.
    fn write_mirror_entry(
        &mut self,
        config_store: &ConfigStore,
        workspace: &WorkspaceName,
        row: &InstalledSource,
    ) -> bool {
        if let Err(error) = config_store.upsert_source_unlocked(workspace, row.clone()) {
            warn!(
                "the config mirror for source '{}' in workspace '{workspace}' could not be brought up to its database row: {error}",
                row.name
            );
            return false;
        }
        self.ledger.record_entry(workspace, &row.name, row);
        true
    }

    /// Rebuilds the artifact caches whose rows this host is missing or behind.
    ///
    /// Best-effort throughout: a cache that cannot be restored is warned about
    /// and left as it is, because the row it came from is still the record.
    async fn hydrate_missing_artifact_caches(
        &mut self,
        workspace: &WorkspaceName,
    ) -> Result<(), AppError> {
        let mut session = self.db;
        let rows = session.sources().list_workspace_sources(workspace).await?;
        for row in rows {
            let mut session = self.db;
            let manifest = session.source_manifests().get(workspace, &row.name).await?;
            if let Some(manifest) = manifest {
                self.hydrate_manifest_cache(workspace, &row.name, &manifest);
            }
            let mut session = self.db;
            let materialization = session.materializations().get(workspace, &row.name).await?;
            if let Some(materialization) = materialization {
                self.hydrate_materialization_cache(workspace, &row.name, &materialization);
            }
        }
        Ok(())
    }

    /// Restores one `manifest.yaml` from the row that is its record.
    ///
    /// An absent file is written outright. A file that disagrees with the row
    /// is overwritten in place only when the ledger proves the bytes are
    /// Coral's own stale cache — the routine cross-host update, which stays
    /// silent and leaves no litter. Anything else has no provable owner, so it
    /// is preserved under a timestamped sibling name before the row's copy
    /// lands: hydration never silently destroys an edit.
    fn hydrate_manifest_cache(
        &mut self,
        workspace: &WorkspaceName,
        source_name: &SourceName,
        record: &SourceManifestRecord,
    ) {
        let path = self.layout.manifest_file(workspace, source_name);
        let on_disk = match std::fs::read(&path) {
            Ok(bytes) => Some(sha256_hex(&bytes)),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => {
                warn!(
                    "leaving the manifest cache for source '{source_name}' in workspace '{workspace}' alone: it could not be read: {error}"
                );
                return;
            }
        };
        if on_disk.as_deref() == Some(record.manifest_hash.as_str()) {
            self.ledger
                .record_manifest(workspace, source_name, &record.manifest_hash);
            return;
        }
        if on_disk.is_some_and(|on_disk| {
            !self
                .ledger
                .matches_manifest(workspace, source_name, &on_disk)
        }) {
            match set_aside_diverged_file(&path) {
                Ok(kept) => warn!(
                    "the manifest at '{}' for source '{source_name}' in workspace '{workspace}' is not the one the database holds and this host cannot prove Coral wrote it; keeping it as '{}' and restoring the database copy",
                    path.display(),
                    kept.display()
                ),
                Err(error) => {
                    warn!(
                        "leaving the manifest at '{}' for source '{source_name}' in workspace '{workspace}' alone: it could not be set aside: {error}",
                        path.display()
                    );
                    return;
                }
            }
        }
        if let Err(error) = write_manifest_cache(&path, &record.manifest_yaml) {
            warn!(
                "the manifest cache for source '{source_name}' in workspace '{workspace}' could not be restored from the database: {error}"
            );
            return;
        }
        self.ledger
            .record_manifest(workspace, source_name, &record.manifest_hash);
        self.report.hydrated_caches += 1;
    }

    /// Restores one `materialized/v4` directory from the row that is its
    /// record, when it is absent or the row's fingerprint is not the one on
    /// disk.
    ///
    /// Freshness is a byte comparison of the fingerprint rather than of the
    /// manifest hash: a re-materialization at unchanged manifest bytes still
    /// moves the fingerprint's descriptor hash and generator versions, and
    /// byte-identical fingerprints mean identical inputs and generators, so
    /// there is nothing to refresh. A row without a fingerprint can prove
    /// nothing either way, so it hydrates on absence only and never loops.
    fn hydrate_materialization_cache(
        &mut self,
        workspace: &WorkspaceName,
        source_name: &SourceName,
        record: &MaterializationRecord,
    ) {
        let materialized_dir = self.layout.v4_materialized_dir(workspace, source_name);
        let fingerprint_file = self.layout.v4_fingerprint_file(workspace, source_name);
        let stale = match std::fs::read_to_string(fingerprint_file) {
            Ok(on_disk) => record
                .fingerprint_yaml
                .as_ref()
                .is_some_and(|row| *row != on_disk),
            Err(_) => record.fingerprint_yaml.is_some(),
        };
        if materialized_dir.exists() && !stale {
            return;
        }
        if let Err(error) =
            hydrate_v4_materialization_cache(self.layout, workspace, source_name, record)
        {
            warn!(
                "the materialized cache for source '{source_name}' in workspace '{workspace}' could not be restored from the database: {error}"
            );
            return;
        }
        self.report.hydrated_caches += 1;
    }
}

/// Moves a file this host cannot prove Coral wrote out of the way, preserved
/// under a timestamped sibling name, and reports where it went.
///
/// The timestamp carries sub-second precision so that a second set-aside can
/// never land on — and destroy — the first.
fn set_aside_diverged_file(path: &Path) -> Result<PathBuf, AppError> {
    let mut kept_name = path.file_name().unwrap_or_default().to_os_string();
    kept_name.push(format!(
        ".diverged-{}",
        Utc::now().format("%Y%m%dT%H%M%S%.9fZ")
    ));
    let kept = path.with_file_name(kept_name);
    std::fs::rename(path, &kept)?;
    Ok(kept)
}

fn write_manifest_cache(path: &Path, manifest_yaml: &str) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        storage_fs::ensure_dir(parent)?;
    }
    storage_fs::write_atomic(path, manifest_yaml.as_bytes())?;
    Ok(())
}

fn remove_legacy_task_jsonl(
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<(), AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    layout.remove_legacy_task_event_logs()?;
    Ok(())
}

async fn cutover_legacy_workspace_catalog(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<WorkspaceCatalogCutoverReport, AppError> {
    cutover_legacy_workspace_catalog_at(db, config_store, layout, now_unix_nanos_i64()?).await
}

async fn cutover_legacy_workspace_catalog_at(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
    now_unix_nanos: i64,
) -> Result<WorkspaceCatalogCutoverReport, AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    let mut tx = db.begin().await?;
    if !tx
        .state_migrations()
        .try_claim(WORKSPACE_CATALOG_CUTOVER_ID, now_unix_nanos)
        .await?
    {
        tx.rollback().await?;
        let mut session = db;
        return Ok(WorkspaceCatalogCutoverReport {
            workspace_count: session.workspaces().list().await?.len(),
            cutover_performed: false,
        });
    }

    let mut workspaces = config_store
        .load_config_unlocked()?
        .legacy_workspace_records();
    if workspaces.is_empty() {
        workspaces = implicitly_provisioned_workspaces(layout)?;
    }
    let workspace_count = workspaces.len();

    tx.workspaces().delete_all().await?;
    import_legacy_workspaces(&mut tx, &workspaces, now_unix_nanos).await?;
    verify_workspace_parity(&mut tx, &workspaces).await?;
    tx.commit().await?;

    Ok(WorkspaceCatalogCutoverReport {
        workspace_count,
        cutover_performed: true,
    })
}

/// Lists the workspaces on-disk state proves an install had, for a legacy
/// config that names none itself.
///
/// Workspaces were once implicit: the catalog seeded one, so a `config.toml`
/// with no workspace tables still described an install that had a workspace,
/// with sources, tasks, and search state under its directory. Nothing records
/// that name any more except the directory itself, so the cutover reads it
/// from there. It does not fall back to a fixed `default`: a genuinely fresh
/// install has no workspace directory and must cut over to no workspaces.
///
/// Every entry here is read as a live workspace, and one class of them is
/// not: a deletion stages the workspace directory into its own root, outside
/// this one, but only after the deletion has committed, and staging that fails
/// only warns. A directory such a deletion left behind — or an older Coral
/// staged beside the live workspaces and failed to remove — carries no
/// evidence that it was deleted, so this scan resurrects it. That window is
/// deliberately open rather than closed: staging before the commit would shut
/// it, at the price of a crash between the rename and the commit leaving a
/// live workspace whose directory is already gone, which is worse than a
/// directory that outlives its workspace. Nothing on disk can tell the two
/// apart after the fact.
///
/// Deliberately a fallback and not a union with the config's own names, which
/// leaves one residual open. A config that named `analytics` and nothing else
/// could still have had a live implicit workspace beside it, and that one is
/// orphaned for good because the cutover marker never re-runs. Closing it by
/// unioning would resurrect exactly the orphans described above, and
/// `cuts_over_legacy_workspaces_into_database` pins that a leftover directory
/// must not come back beside a config that names workspaces. The exposed
/// population is narrow: every config Coral itself persisted serializes its
/// workspaces back, so only a hand-edited config reaches this shape.
fn implicitly_provisioned_workspaces(
    layout: &AppStateLayout,
) -> Result<Vec<WorkspaceRecord>, AppError> {
    let entries = match std::fs::read_dir(layout.workspaces_root()) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let mut workspaces = Vec::new();
    for entry in entries {
        let entry = entry?;
        // Metadata, not `file_type`: a workspace directory may be a symlink to
        // another volume, and skipping it here would orphan it for good once
        // the cutover marker commits. Following the link means a dangling one
        // reports `NotFound`, which is this scan's answer for "not a workspace
        // directory" rather than a reason to fail startup; every other io error
        // still surfaces.
        match std::fs::metadata(entry.path()) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        }
        let directory_name = entry.file_name();
        let Some(name) = directory_name
            .to_str()
            .and_then(|name| WorkspaceName::parse(name).ok())
        else {
            continue;
        };
        workspaces.push(WorkspaceRecord { name });
    }
    workspaces.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(workspaces)
}

async fn import_legacy_workspaces<S>(
    session: &mut S,
    workspaces: &[WorkspaceRecord],
    now_unix_nanos: i64,
) -> Result<(), AppError>
where
    S: DbRepos,
{
    for workspace in workspaces {
        session
            .workspaces()
            .ensure(workspace.name.as_str(), now_unix_nanos)
            .await?;
    }
    Ok(())
}

async fn verify_workspace_parity<S>(
    session: &mut S,
    legacy_workspaces: &[WorkspaceRecord],
) -> Result<(), AppError>
where
    S: DbRepos,
{
    let expected = legacy_workspaces
        .iter()
        .map(|workspace| workspace.name.as_str().to_string())
        .collect::<BTreeSet<_>>();
    let actual = session
        .workspaces()
        .list()
        .await?
        .into_iter()
        .map(|workspace| workspace.id)
        .collect::<BTreeSet<_>>();
    if actual == expected {
        return Ok(());
    }
    Err(AppError::Database(format!(
        "workspace catalog cutover parity validation failed: legacy={expected:?} database={actual:?}"
    )))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::{
        InstalledSource, MirrorLedger, SourceImportReport, SourceName, SourceOrigin,
        WORKSPACE_CATALOG_CUTOVER_ID, WorkspaceCatalogCutoverReport,
        cutover_legacy_workspace_catalog, cutover_legacy_workspace_catalog_at,
        reconcile_source_state, resolve_installed_manifest, run_state_migrations,
    };
    use crate::credentials::{
        CredentialManager, CredentialSetId, CredentialStorageKind, CredentialStore,
    };
    use crate::sources::materialization::{
        FINGERPRINT_FILENAME, OPERATION_METADATA_FILENAME, PROJECTIONS_FILENAME,
        SEMANTIC_IR_FILENAME, SOURCE_DOCUMENT_RAW_FILENAME, SOURCE_DOCUMENT_YAML_FILENAME,
    };
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::storage::fs::DELETION_BACKUP_INFIX;
    use crate::workspaces::WorkspaceName;

    /// The unique suffix a staged deletion carries, fixed so the directories
    /// these tests plant read exactly as `move_for_delete` would have written
    /// them.
    const STAGED_DELETION_SUFFIX: &str = "7f1c5a4e-1d29-4f3a-9f2b-2c6d0f9a1b34";

    #[tokio::test]
    async fn cuts_over_legacy_workspaces_into_database() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let analytics_workspace = WorkspaceName::parse("analytics").expect("workspace");
        config_store
            .create_legacy_workspace_entry_for_tests(&analytics_workspace)
            .expect("create legacy workspace entry");
        // A config that names workspaces is authoritative, so a directory left
        // behind by a deleted workspace must not come back beside them.
        let deleted_workspace = WorkspaceName::parse("removed").expect("workspace");
        std::fs::create_dir_all(layout.workspace_dir(&deleted_workspace))
            .expect("create leftover workspace dir");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true
            }
        );
        assert_eq!(
            workspace_ids(&db).await,
            vec!["analytics".to_string()],
            "the cutover carries the legacy names across and invents none"
        );
        let mut session = &db;
        assert!(
            session
                .state_migrations()
                .has_completed(WORKSPACE_CATALOG_CUTOVER_ID)
                .await
                .expect("read cutover marker")
        );
    }

    #[tokio::test]
    async fn cutover_resets_stale_shadow_database_rows() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let analytics_workspace = WorkspaceName::parse("analytics").expect("workspace");
        config_store
            .create_legacy_workspace_entry_for_tests(&analytics_workspace)
            .expect("create legacy workspace entry");
        let db = open_sqlite(&layout).await;
        let mut tx = db.begin().await.expect("begin stale seed tx");
        tx.workspaces()
            .ensure("stale", 7)
            .await
            .expect("seed stale workspace");
        tx.commit().await.expect("commit stale seed tx");

        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        let mut session = &db;
        assert_eq!(
            session
                .workspaces()
                .list()
                .await
                .expect("list workspaces")
                .into_iter()
                .map(|workspace| workspace.id)
                .collect::<Vec<_>>(),
            vec!["analytics".to_string()]
        );
    }

    #[tokio::test]
    async fn completed_cutover_does_not_reimport_legacy_config() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("initial cutover");
        std::fs::write(layout.config_file(), "[[workspaces]\n").expect("corrupt config");

        let report = cutover_legacy_workspace_catalog(&db, &config_store, &layout)
            .await
            .expect("marker should skip legacy config reload");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 0,
                cutover_performed: false
            }
        );
    }

    /// A fresh install names no workspace and holds none on disk, so the
    /// cutover must not seed one on its way into the database.
    #[tokio::test]
    async fn cutover_without_legacy_workspaces_creates_none() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        // An install that has held a workspace and lost it looks the same as
        // one that never had one: an emptied root, and at most a directory a
        // deletion staged aside — into its own root — and failed to remove.
        std::fs::create_dir_all(layout.workspaces_root()).expect("create workspaces root");
        std::fs::create_dir_all(layout.deleted_workspaces_root().join(format!(
            "default{DELETION_BACKUP_INFIX}{STAGED_DELETION_SUFFIX}"
        )))
        .expect("stage a deletion that was never removed");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 0,
                cutover_performed: true
            }
        );
        let mut session = &db;
        assert!(
            session
                .workspaces()
                .list()
                .await
                .expect("list workspaces")
                .is_empty()
        );
    }

    /// Workspaces were once implicit, so an older install can hold one whose
    /// name only its directory records. The cutover happens once and marks
    /// itself done, so a workspace it drops is orphaned for good: its name and
    /// its contents have to come across.
    #[tokio::test]
    async fn cutover_preserves_an_implicitly_provisioned_legacy_workspace() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let implicit_workspace = WorkspaceName::parse("default").expect("workspace");
        let installed_source = layout
            .sources_root(&implicit_workspace)
            .join("github")
            .join("manifest.yaml");
        std::fs::create_dir_all(installed_source.parent().expect("source dir"))
            .expect("create legacy source dir");
        std::fs::write(&installed_source, "name: github").expect("write legacy manifest");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true
            }
        );
        assert_eq!(workspace_ids(&db).await, vec!["default".to_string()]);
        assert!(
            installed_source.exists(),
            "the preserved workspace keeps its contents"
        );
    }

    /// A workspace directory can be a symlink — relocated to another volume by
    /// hand, say. The cutover runs once, so reading only the link itself and
    /// not what it points at would orphan that workspace for good.
    #[cfg(unix)]
    #[tokio::test]
    async fn cutover_preserves_a_symlinked_legacy_workspace() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let implicit_workspace = WorkspaceName::parse("default").expect("workspace");
        let relocated = temp.path().join("other-volume").join("default");
        std::fs::create_dir_all(&relocated).expect("create relocated workspace dir");
        std::fs::create_dir_all(layout.workspaces_root()).expect("create workspaces root");
        std::os::unix::fs::symlink(&relocated, layout.workspace_dir(&implicit_workspace))
            .expect("link the relocated workspace into place");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true
            }
        );
        assert_eq!(workspace_ids(&db).await, vec!["default".to_string()]);
    }

    /// Following the link is what carries a relocated workspace across, and a
    /// link whose target is gone is the cost of that. It names no workspace, so
    /// the cutover skips it — refusing to start over a broken link would strand
    /// the whole install.
    #[cfg(unix)]
    #[tokio::test]
    async fn cutover_skips_a_dangling_workspace_symlink() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let live_workspace = WorkspaceName::parse("analytics").expect("workspace");
        let dangling_workspace = WorkspaceName::parse("relocated").expect("workspace");
        std::fs::create_dir_all(layout.workspace_dir(&live_workspace))
            .expect("create the live workspace dir");
        std::os::unix::fs::symlink(
            temp.path().join("other-volume").join("relocated"),
            layout.workspace_dir(&dangling_workspace),
        )
        .expect("link a workspace that is no longer there");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                cutover_performed: true
            }
        );
        assert_eq!(workspace_ids(&db).await, vec!["analytics".to_string()]);
    }

    /// A workspace name is free to look exactly like a staged deletion, so a
    /// name can never say which of the two a directory is. Location can: the
    /// live workspace stays in the workspaces root and the staged deletion
    /// sits in its own, and only the former is imported.
    #[tokio::test]
    async fn cutover_separates_a_staged_deletion_from_a_workspace_named_like_one() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let named_like_a_backup =
            format!("analytics{DELETION_BACKUP_INFIX}{STAGED_DELETION_SUFFIX}");
        let implicit_workspace = WorkspaceName::parse(&named_like_a_backup).expect("workspace");
        std::fs::create_dir_all(layout.workspace_dir(&implicit_workspace))
            .expect("create the legacy workspace dir");
        std::fs::create_dir_all(layout.deleted_workspaces_root().join(format!(
            "work{DELETION_BACKUP_INFIX}{STAGED_DELETION_SUFFIX}"
        )))
        .expect("stage a deletion outside the workspaces root");
        let db = open_sqlite(&layout).await;

        cutover_legacy_workspace_catalog_at(&db, &config_store, &layout, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(workspace_ids(&db).await, vec![named_like_a_backup]);
    }

    async fn workspace_ids(db: &CoralDb) -> Vec<String> {
        let mut session = db;
        session
            .workspaces()
            .list()
            .await
            .expect("list workspaces")
            .into_iter()
            .map(|workspace| workspace.id)
            .collect()
    }

    #[tokio::test]
    async fn shared_database_does_not_scope_task_cleanup_to_the_first_layout() {
        let temp = tempdir().expect("temp dir");
        let first_layout =
            AppStateLayout::discover(Some(temp.path().join("first"))).expect("first layout");
        let second_layout =
            AppStateLayout::discover(Some(temp.path().join("second"))).expect("second layout");
        first_layout.ensure().expect("ensure first layout");
        second_layout.ensure().expect("ensure second layout");
        let first_config_store = ConfigStore::new(first_layout.clone());
        let second_config_store = ConfigStore::new(second_layout.clone());
        let legacy_workspace = WorkspaceName::parse("analytics").expect("workspace");
        let first_legacy_file = first_layout
            .workspace_dir(&legacy_workspace)
            .join("tasks")
            .join("tasks.jsonl");
        let second_legacy_file = second_layout
            .workspace_dir(&legacy_workspace)
            .join("tasks")
            .join("tasks.jsonl");
        for path in [&first_legacy_file, &second_legacy_file] {
            std::fs::create_dir_all(path.parent().expect("legacy task dir"))
                .expect("create legacy task dir");
            std::fs::write(path, "sensitive task intent").expect("write legacy task file");
        }
        let db = open_sqlite(&first_layout).await;

        run_state_migrations(&db, &first_config_store, &first_layout)
            .await
            .expect("run migrations for first layout");
        run_state_migrations(&db, &second_config_store, &second_layout)
            .await
            .expect("run migrations for second layout");

        assert!(!first_legacy_file.exists());
        assert!(!second_legacy_file.exists());
    }

    /// One legacy file-based install: a config file, its source tree, and the
    /// database a boot would reconcile it with.
    struct ImportFixture {
        _temp: tempfile::TempDir,
        layout: AppStateLayout,
        config_store: ConfigStore,
        db: CoralDb,
    }

    impl ImportFixture {
        async fn new() -> Self {
            Self::open(None).await
        }

        /// A second config directory reconciling against this one's database.
        ///
        /// One file-backed `SQLite` file stands in for the shared server
        /// database: what makes a host a host in every rule here is its own
        /// config file, ledger, and source tree, not how it reaches the rows.
        async fn peer_host(&self) -> Self {
            Self::open(Some(self.layout.database_file())).await
        }

        /// A peer host that has already reconciled the same install once: it
        /// holds the same files and a ledger that records them, which is what
        /// later makes its disagreements with the database provable.
        async fn reconciled_peer_host(
            &self,
            workspace: &WorkspaceName,
            source: &InstalledSource,
        ) -> Self {
            let peer = self.peer_host().await;
            peer.declare_workspace(workspace);
            peer.install(workspace, source);
            peer.import().await;
            peer
        }

        async fn open(shared_database: Option<PathBuf>) -> Self {
            let temp = tempdir().expect("temp dir");
            let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
            layout.ensure().expect("ensure layout");
            let config_store = ConfigStore::new(layout.clone());
            let db = match shared_database {
                Some(path) => open_sqlite_at(path).await,
                None => open_sqlite(&layout).await,
            };
            Self {
                _temp: temp,
                layout,
                config_store,
                db,
            }
        }

        /// Declares a workspace in `config.toml` and gives it a database row,
        /// the shape an install that has already cut over is in.
        async fn cut_over_workspace(&self, workspace: &WorkspaceName) {
            self.declare_workspace(workspace);
            cutover_legacy_workspace_catalog(&self.db, &self.config_store, &self.layout)
                .await
                .expect("cut over the legacy workspace catalog");
        }

        fn declare_workspace(&self, workspace: &WorkspaceName) {
            self.config_store
                .create_legacy_workspace_entry_for_tests(workspace)
                .expect("declare legacy workspace");
        }

        fn write_entry(&self, workspace: &WorkspaceName, source: &InstalledSource) {
            self.config_store
                .upsert_source(workspace, source.clone())
                .expect("write config entry");
        }

        fn write_manifest(&self, workspace: &WorkspaceName, name: &SourceName, yaml: &str) {
            let path = self.layout.manifest_file(workspace, name);
            std::fs::create_dir_all(path.parent().expect("source dir")).expect("create source dir");
            std::fs::write(path, yaml).expect("write manifest");
        }

        fn write_materialization(
            &self,
            workspace: &WorkspaceName,
            name: &SourceName,
            fingerprint: Option<&str>,
        ) {
            let dir = self.layout.v4_materialized_dir(workspace, name);
            std::fs::create_dir_all(&dir).expect("create materialized dir");
            for (file, contents) in [
                (PROJECTIONS_FILENAME, "projections: []"),
                (SOURCE_DOCUMENT_RAW_FILENAME, "raw"),
                (SOURCE_DOCUMENT_YAML_FILENAME, "document: {}"),
                (SEMANTIC_IR_FILENAME, "ir: {}"),
                (OPERATION_METADATA_FILENAME, "operations: {}"),
            ] {
                std::fs::write(dir.join(file), contents).expect("write artifact");
            }
            if let Some(fingerprint) = fingerprint {
                std::fs::write(dir.join(FINGERPRINT_FILENAME), fingerprint)
                    .expect("write fingerprint");
            }
        }

        /// Installs one source the way a previous boot would have left it:
        /// config entry, manifest file, and a materialized directory.
        fn install(&self, workspace: &WorkspaceName, source: &InstalledSource) {
            self.write_entry(workspace, source);
            self.write_manifest(
                workspace,
                &source.name,
                &manifest_yaml(source.name.as_str()),
            );
            self.write_materialization(workspace, &source.name, Some("fingerprint: {}"));
        }

        /// Writes rows another host would have written, leaving this host's
        /// ledger with no record of them.
        async fn seed_rows(&self, workspace: &WorkspaceName, source: &InstalledSource) {
            let mut tx = self.db.begin().await.expect("begin seed tx");
            tx.sources()
                .upsert_source(workspace, source, 7)
                .await
                .expect("seed source row");
            tx.source_manifests()
                .upsert(
                    workspace,
                    &source.name,
                    &manifest_yaml(source.name.as_str()),
                    7,
                )
                .await
                .expect("seed manifest row");
            tx.commit().await.expect("commit seed tx");
        }

        /// Removes one source the way another host sharing this database would:
        /// the row goes, the tombstone lands, and this host's files stay.
        async fn delete_rows_elsewhere(&self, workspace: &WorkspaceName, name: &SourceName) {
            let mut tx = self.db.begin().await.expect("begin delete tx");
            tx.sources()
                .remove_source(workspace, name, 9)
                .await
                .expect("remove source row");
            tx.commit().await.expect("commit delete tx");
        }

        async fn import(&self) -> SourceImportReport {
            reconcile_source_state(&self.db, &self.config_store, &self.layout)
                .await
                .expect("import legacy source state")
        }

        async fn row(
            &self,
            workspace: &WorkspaceName,
            name: &SourceName,
        ) -> Option<InstalledSource> {
            let mut session = &self.db;
            session
                .sources()
                .get_source(workspace, name)
                .await
                .expect("read source row")
        }

        async fn manifest_row(
            &self,
            workspace: &WorkspaceName,
            name: &SourceName,
        ) -> Option<String> {
            let mut session = &self.db;
            session
                .source_manifests()
                .get(workspace, name)
                .await
                .expect("read manifest row")
                .map(|record| record.manifest_yaml)
        }

        async fn materialization_row(
            &self,
            workspace: &WorkspaceName,
            name: &SourceName,
        ) -> Option<crate::state::db::MaterializationRecord> {
            let mut session = &self.db;
            session
                .materializations()
                .get(workspace, name)
                .await
                .expect("read materialization row")
        }

        async fn tombstoned(&self, workspace: &WorkspaceName, name: &SourceName) -> bool {
            let mut session = &self.db;
            session
                .sources()
                .is_tombstoned(workspace, name)
                .await
                .expect("read tombstone")
        }

        fn ledger(&self) -> MirrorLedger {
            MirrorLedger::load(self.layout.config_file())
        }

        fn config_bytes(&self) -> Vec<u8> {
            std::fs::read(self.layout.config_file()).expect("read config file")
        }

        fn entry(&self, workspace: &WorkspaceName, name: &SourceName) -> Option<InstalledSource> {
            self.config_store.get_source(workspace, name).ok()
        }

        fn manifest_bytes(&self, workspace: &WorkspaceName, name: &SourceName) -> Option<Vec<u8>> {
            std::fs::read(self.layout.manifest_file(workspace, name)).ok()
        }

        /// Every file in the source's directory tree, keyed by its path
        /// relative to that directory — what "restored byte-for-byte" means.
        fn source_files(
            &self,
            workspace: &WorkspaceName,
            name: &SourceName,
        ) -> BTreeMap<PathBuf, Vec<u8>> {
            let root = self.layout.source_dir(workspace, name);
            let mut files = BTreeMap::new();
            let mut pending = vec![root.clone()];
            while let Some(dir) = pending.pop() {
                for entry in std::fs::read_dir(&dir).expect("read source dir") {
                    let path = entry.expect("dir entry").path();
                    if path.is_dir() {
                        pending.push(path);
                    } else {
                        let relative = path.strip_prefix(&root).expect("relative path").to_owned();
                        files.insert(relative, std::fs::read(&path).expect("read artifact"));
                    }
                }
            }
            files
        }

        /// The manifests hydration preserved rather than overwrote.
        fn set_aside_manifests(
            &self,
            workspace: &WorkspaceName,
            name: &SourceName,
        ) -> Vec<std::path::PathBuf> {
            let mut kept = self
                .source_files(workspace, name)
                .into_keys()
                .filter(|path| path.to_string_lossy().contains(".diverged-"))
                .collect::<Vec<_>>();
            kept.sort();
            kept
        }

        fn fingerprint(&self, workspace: &WorkspaceName, name: &SourceName) -> Option<String> {
            std::fs::read_to_string(self.layout.v4_fingerprint_file(workspace, name)).ok()
        }

        /// Replays another host's re-materialization of an unchanged manifest:
        /// only the fingerprint the row carries moves.
        async fn rematerialize_elsewhere(
            &self,
            workspace: &WorkspaceName,
            name: &SourceName,
            fingerprint_yaml: &str,
        ) {
            let mut record = self
                .materialization_row(workspace, name)
                .await
                .expect("materialization row");
            record.fingerprint_yaml = Some(fingerprint_yaml.to_string());
            let mut tx = self.db.begin().await.expect("begin rematerialize tx");
            tx.materializations()
                .upsert(workspace, name, &record, 13)
                .await
                .expect("rewrite materialization row");
            tx.commit().await.expect("commit rematerialize tx");
        }

        /// Removes a workspace the way another host sharing this database
        /// would: the row and everything it cascades go, this host's files stay.
        async fn delete_workspace_elsewhere(&self, workspace: &WorkspaceName) {
            let mut tx = self.db.begin().await.expect("begin workspace delete tx");
            tx.workspaces()
                .delete(workspace.as_str())
                .await
                .expect("delete workspace row");
            tx.commit().await.expect("commit workspace delete tx");
        }
    }

    fn analytics() -> WorkspaceName {
        WorkspaceName::parse("analytics").expect("workspace name")
    }

    fn imported_source(name: &str) -> InstalledSource {
        InstalledSource {
            name: SourceName::parse(name).expect("source name"),
            version: Some("0.1.0".to_string()),
            variables: BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            credential_revision: uuid::Uuid::nil(),
            origin: SourceOrigin::Imported,
        }
    }

    fn manifest_yaml(name: &str) -> String {
        format!(
            r"
name: {name}
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: rows
    description: Rows
    request:
      method: GET
      path: /rows
    response: {{}}
    columns:
      - name: id
        type: Utf8
"
        )
    }

    #[tokio::test]
    async fn a_fresh_install_imports_nothing() {
        let fixture = ImportFixture::new().await;

        assert_eq!(fixture.import().await, SourceImportReport::default());
    }

    #[tokio::test]
    async fn a_populated_config_imports_once_and_then_reports_it_present() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        fixture.install(&workspace, &source);
        let config_before = fixture.config_bytes();

        let first = fixture.import().await;

        assert_eq!(first.imported, 1);
        assert_eq!(
            fixture.row(&workspace, &source.name).await,
            Some(source.clone())
        );
        assert_eq!(
            fixture.manifest_row(&workspace, &source.name).await,
            Some(manifest_yaml("reports"))
        );
        assert!(
            fixture
                .materialization_row(&workspace, &source.name)
                .await
                .is_some()
        );
        assert!(
            fixture
                .ledger()
                .matches_entry(&workspace, &source.name, &source)
        );

        let second = fixture.import().await;

        assert_eq!(second.imported, 0);
        assert_eq!(second.already_present, 1);
        assert_eq!(
            fixture.config_bytes(),
            config_before,
            "the import must never edit config.toml"
        );
    }

    /// There is no marker, so a source an older binary wrote to `config.toml`
    /// after the first boot has to come across at the next one.
    #[tokio::test]
    async fn a_source_added_after_the_first_boot_imports_on_the_next_boot() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        fixture.import().await;

        let source = imported_source("reports");
        fixture.install(&workspace, &source);

        assert_eq!(fixture.import().await.imported, 1);
        assert_eq!(fixture.row(&workspace, &source.name).await, Some(source));
    }

    /// A ledger record the entry no longer matches proves the entry was
    /// rewritten on this host since Coral reconciled it, so the files win.
    #[tokio::test]
    async fn an_entry_rewritten_since_reconciliation_imports_as_an_update() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let mut source = imported_source("reports");
        fixture.install(&workspace, &source);
        fixture.import().await;

        source
            .variables
            .insert("region".to_string(), "eu".to_string());
        fixture.write_entry(&workspace, &source);
        let updated_manifest = manifest_yaml("reports").replace("version: 0.1.0", "version: 0.2.0");
        fixture.write_manifest(&workspace, &source.name, &updated_manifest);

        let report = fixture.import().await;

        assert_eq!(report.updated_from_files, 1);
        assert_eq!(
            fixture.row(&workspace, &source.name).await,
            Some(source.clone())
        );
        assert_eq!(
            fixture.manifest_row(&workspace, &source.name).await,
            Some(updated_manifest.clone())
        );
        let ledger = fixture.ledger();
        assert!(ledger.matches_entry(&workspace, &source.name, &source));
        assert!(ledger.matches_manifest(
            &workspace,
            &source.name,
            &crate::hash::sha256_hex(updated_manifest.as_bytes())
        ));
    }

    /// A host's first reconciled boot has no ledger. An entry that agrees with
    /// its row is provably in sync, so the record is seeded — including the
    /// manifest record, which is what keeps the hydration pass from setting
    /// aside a cache that is byte-identical to the row.
    #[tokio::test]
    async fn an_unrecorded_entry_that_agrees_with_its_row_seeds_the_ledger() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        fixture.install(&workspace, &source);
        fixture.seed_rows(&workspace, &source).await;

        let report = fixture.import().await;

        assert_eq!(report.already_present, 1);
        assert_eq!(report.imported, 0);
        let ledger = fixture.ledger();
        assert!(ledger.matches_entry(&workspace, &source.name, &source));
        assert!(ledger.matches_manifest(
            &workspace,
            &source.name,
            &crate::hash::sha256_hex(manifest_yaml("reports").as_bytes())
        ));
    }

    /// An unrecorded entry that disagrees with its row has no provable owner,
    /// so neither side moves and the warning is recorded once per content.
    #[tokio::test]
    async fn an_unrecorded_entry_that_differs_from_its_row_is_warned_about_and_preserved() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let entry = imported_source("reports");
        fixture.install(&workspace, &entry);
        let mut row = entry.clone();
        row.version = Some("9.9.9".to_string());
        fixture.seed_rows(&workspace, &row).await;
        let config_before = fixture.config_bytes();

        let report = fixture.import().await;

        assert_eq!(report.divergent_unreconciled, 1);
        assert_eq!(fixture.row(&workspace, &entry.name).await, Some(row));
        assert_eq!(fixture.config_bytes(), config_before);
        let ledger = fixture.ledger();
        assert!(
            !ledger.entry_recorded(&workspace, &entry.name),
            "an unprovable entry must never be stamped as reconciled"
        );
        assert!(
            ledger.matches_divergence_warning(&workspace, &entry.name, &entry),
            "the warning must be recorded so the next boot stays quiet"
        );

        let second = fixture.import().await;

        assert_eq!(second.divergent_unreconciled, 1);
        assert!(!fixture.ledger().entry_recorded(&workspace, &entry.name));
    }

    /// The workspace cutover is one-shot, so a workspace an older binary added
    /// to `config.toml` afterwards has no row. Without one, its sources would
    /// be invisible once reads move to the database.
    #[tokio::test]
    async fn a_config_only_workspace_is_created_and_its_sources_imported() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.declare_workspace(&workspace);
        let source = imported_source("reports");
        fixture.install(&workspace, &source);

        let report = fixture.import().await;

        assert_eq!(report.workspaces_created, 1);
        assert_eq!(report.imported, 1);
        assert_eq!(
            workspace_ids(&fixture.db).await,
            vec!["analytics".to_string()]
        );
        assert_eq!(fixture.row(&workspace, &source.name).await, Some(source));
    }

    /// A workspace this host reconciled and the database no longer holds was
    /// deleted through the database. Resurrecting it from this host's stale
    /// config would undo that deletion at every boot.
    #[tokio::test]
    async fn a_config_only_workspace_the_ledger_recorded_is_skipped_with_its_sources() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.declare_workspace(&workspace);
        let source = imported_source("reports");
        fixture.install(&workspace, &source);
        let mut ledger = MirrorLedger::default();
        ledger.record_workspace(&workspace);
        ledger
            .save(fixture.layout.config_file())
            .expect("save ledger");

        let report = fixture.import().await;

        assert_eq!(report.workspaces_skipped_deleted, 1);
        assert_eq!(report.imported, 0);
        assert!(workspace_ids(&fixture.db).await.is_empty());
        assert!(fixture.row(&workspace, &source.name).await.is_none());
    }

    /// A tombstone plus a ledger-matching entry is another host's deletion
    /// reaching this host's stale mirror; re-adding it would undo the deletion.
    #[tokio::test]
    async fn a_tombstoned_entry_the_ledger_still_matches_is_skipped() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        fixture.install(&workspace, &source);
        fixture.import().await;
        fixture
            .delete_rows_elsewhere(&workspace, &source.name)
            .await;

        let report = fixture.import().await;

        assert_eq!(report.skipped_tombstoned, 1);
        assert!(fixture.row(&workspace, &source.name).await.is_none());
        assert!(fixture.tombstoned(&workspace, &source.name).await);
    }

    /// A tombstoned name whose entry changed since Coral reconciled it is a
    /// genuine re-add — a downgraded binary's, or an operator's.
    #[tokio::test]
    async fn a_tombstoned_entry_rewritten_since_reconciliation_is_readded() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let mut source = imported_source("reports");
        fixture.install(&workspace, &source);
        fixture.import().await;
        fixture
            .delete_rows_elsewhere(&workspace, &source.name)
            .await;

        source
            .variables
            .insert("region".to_string(), "eu".to_string());
        fixture.write_entry(&workspace, &source);
        let report = fixture.import().await;

        assert_eq!(report.readded_after_tombstone, 1);
        assert_eq!(
            fixture.row(&workspace, &source.name).await,
            Some(source.clone())
        );
        assert!(
            !fixture.tombstoned(&workspace, &source.name).await,
            "a re-add must revoke the earlier deletion"
        );
    }

    /// A tombstone this host never reconciled is not a deletion reaching a
    /// stale mirror: compensating a failed fresh install on another host writes
    /// one, and the config entry it would otherwise suppress here is a live
    /// config-only source. Only a ledger record makes the entry a mirror.
    #[tokio::test]
    async fn a_tombstoned_entry_the_ledger_never_recorded_is_readded() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        // Another host installed and compensated the same name, leaving a
        // tombstone; this host only ever had the source in its own files.
        fixture.seed_rows(&workspace, &source).await;
        fixture
            .delete_rows_elsewhere(&workspace, &source.name)
            .await;
        fixture.install(&workspace, &source);
        assert!(
            fixture.tombstoned(&workspace, &source.name).await,
            "the fixture must leave a tombstone for the import to rule on"
        );
        assert!(
            !fixture.ledger().entry_recorded(&workspace, &source.name),
            "this host must never have reconciled the entry"
        );

        let report = fixture.import().await;

        assert_eq!(report.readded_after_tombstone, 1);
        assert_eq!(report.skipped_tombstoned, 0);
        assert_eq!(
            fixture.row(&workspace, &source.name).await,
            Some(source.clone())
        );
        assert!(
            !fixture.tombstoned(&workspace, &source.name).await,
            "importing the entry must revoke the unreconciled deletion"
        );
    }

    /// One unreadable source must not cost the install its other sources, nor
    /// its startup.
    #[tokio::test]
    async fn a_source_whose_manifest_cannot_be_read_is_skipped_and_the_rest_import() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let healthy = imported_source("reports");
        fixture.install(&workspace, &healthy);
        let broken = imported_source("broken");
        fixture.write_entry(&workspace, &broken);
        fixture.write_manifest(&workspace, &broken.name, "this is not a manifest");

        let report = fixture.import().await;

        assert_eq!(report.imported, 1);
        assert_eq!(report.skipped_invalid, 1);
        assert_eq!(fixture.row(&workspace, &healthy.name).await, Some(healthy));
        assert!(
            fixture.row(&workspace, &broken.name).await.is_none(),
            "the rolled-back transaction must leave no half-imported source"
        );
        assert!(!fixture.ledger().entry_recorded(&workspace, &broken.name));
    }

    /// Legacy data Coral cannot parse is never a reason to refuse to start.
    #[tokio::test]
    async fn an_unreadable_config_file_does_not_fail_startup() {
        let fixture = ImportFixture::new().await;
        std::fs::write(fixture.layout.config_file(), "[[workspaces]\n").expect("corrupt config");

        assert_eq!(fixture.import().await, SourceImportReport::default());
    }

    /// The ledger is proof, not state of record: losing it costs a re-import,
    /// never a boot.
    #[tokio::test]
    async fn a_corrupt_ledger_is_ignored_and_the_import_still_runs() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        fixture.install(&workspace, &source);
        let ledger_path = fixture.layout.config_file().with_extension("toml.ledger");
        std::fs::write(&ledger_path, b"{ not json at all").expect("corrupt ledger");

        assert_eq!(fixture.import().await.imported, 1);
        assert!(
            fixture
                .ledger()
                .matches_entry(&workspace, &source.name, &source)
        );
    }

    /// The fingerprint is optional to the v4 loader, so a materialization
    /// written without one imports with a null fingerprint rather than being
    /// dropped.
    #[tokio::test]
    async fn a_materialization_without_a_fingerprint_imports_with_a_null_one() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        fixture.write_entry(&workspace, &source);
        fixture.write_manifest(&workspace, &source.name, &manifest_yaml("reports"));
        fixture.write_materialization(&workspace, &source.name, None);

        assert_eq!(fixture.import().await.imported, 1);
        let materialization = fixture
            .materialization_row(&workspace, &source.name)
            .await
            .expect("materialization row");
        assert_eq!(materialization.fingerprint_yaml, None);
        assert_eq!(materialization.projections_yaml, "projections: []");
    }

    /// The rows are the record; the files are a cache. Losing the cache costs
    /// a boot, not the source — and what comes back is the same bytes the
    /// manifest and materialization readers were serving before.
    #[tokio::test]
    async fn a_deleted_artifact_cache_is_restored_from_its_rows() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        fixture.install(&workspace, &source);
        fixture.import().await;
        let installed = fixture.source_files(&workspace, &source.name);

        std::fs::remove_file(fixture.layout.manifest_file(&workspace, &source.name))
            .expect("delete manifest");
        std::fs::remove_dir_all(fixture.layout.v4_materialized_dir(&workspace, &source.name))
            .expect("delete materialization");
        let report = fixture.import().await;

        assert_eq!(report.hydrated_caches, 2);
        assert_eq!(
            fixture.source_files(&workspace, &source.name),
            installed,
            "hydration must restore the artifact files byte-for-byte"
        );
    }

    /// The routine cross-host update: one host updates a source, the other
    /// boots. Its stale mirror entry and its stale cache are both provably
    /// Coral's own, so both are brought forward silently — no set-aside file,
    /// and a downgrade catalog that is current rather than frozen.
    #[tokio::test]
    async fn an_update_on_one_host_refreshes_the_other_hosts_mirror_and_cache() {
        let first = ImportFixture::new().await;
        let workspace = analytics();
        first.cut_over_workspace(&workspace).await;
        let mut source = imported_source("reports");
        first.install(&workspace, &source);
        first.import().await;
        let second = first.reconciled_peer_host(&workspace, &source).await;

        source
            .variables
            .insert("region".to_string(), "eu".to_string());
        first.write_entry(&workspace, &source);
        let updated_manifest = manifest_yaml("reports").replace("version: 0.1.0", "version: 0.2.0");
        first.write_manifest(&workspace, &source.name, &updated_manifest);
        assert_eq!(first.import().await.updated_from_files, 1);

        let report = second.import().await;

        assert_eq!(report.mirror_entries_refreshed, 1);
        assert_eq!(report.mirrored_entries, 0);
        assert_eq!(second.entry(&workspace, &source.name), Some(source.clone()));
        assert_eq!(
            second.manifest_bytes(&workspace, &source.name),
            Some(updated_manifest.into_bytes())
        );
        assert!(
            second
                .set_aside_manifests(&workspace, &source.name)
                .is_empty(),
            "a provably stale cache must be refreshed in place, not set aside"
        );
    }

    /// A mirror entry that agrees with neither the row nor the ledger has no
    /// provable owner, so the mirror pass leaves it exactly as the operator
    /// wrote it — the database is what this binary serves either way.
    #[tokio::test]
    async fn an_entry_differing_from_both_its_row_and_the_ledger_is_left_alone() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let entry = imported_source("reports");
        fixture.install(&workspace, &entry);
        let mut row = entry.clone();
        row.version = Some("9.9.9".to_string());
        fixture.seed_rows(&workspace, &row).await;
        let config_before = fixture.config_bytes();

        let report = fixture.import().await;

        assert_eq!(report.mirror_entries_refreshed, 0);
        assert_eq!(report.mirrored_entries, 0);
        assert_eq!(fixture.config_bytes(), config_before);
    }

    /// A manifest this host cannot prove Coral wrote is never overwritten in
    /// place: the edit is preserved beside the file, and the artifact of record
    /// is restored over it.
    #[tokio::test]
    async fn a_hand_edited_manifest_is_set_aside_and_the_row_restored() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        fixture.install(&workspace, &source);
        fixture.import().await;
        let edited = manifest_yaml("reports").replace("Rows", "Hand-edited rows");
        fixture.write_manifest(&workspace, &source.name, &edited);

        let report = fixture.import().await;

        assert_eq!(report.hydrated_caches, 1);
        assert_eq!(
            fixture.manifest_bytes(&workspace, &source.name),
            Some(manifest_yaml("reports").into_bytes())
        );
        let kept = fixture.set_aside_manifests(&workspace, &source.name);
        let [kept] = kept.as_slice() else {
            panic!("the edit must be preserved beside the file, exactly once: {kept:?}");
        };
        let kept_bytes = std::fs::read(
            fixture
                .layout
                .source_dir(&workspace, &source.name)
                .join(kept),
        )
        .expect("read the set-aside manifest");
        assert_eq!(kept_bytes, edited.into_bytes());
    }

    /// A host's first reconciled boot has no ledger, so the import seeds
    /// records for caches that already match their rows. Without that, every
    /// first boot would litter the install with set-aside files.
    #[tokio::test]
    async fn a_first_reconciled_boot_leaves_a_matching_cache_alone() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        fixture.install(&workspace, &source);
        fixture.seed_rows(&workspace, &source).await;

        let report = fixture.import().await;

        assert_eq!(report.already_present, 1);
        assert_eq!(report.hydrated_caches, 0);
        assert!(
            fixture
                .set_aside_manifests(&workspace, &source.name)
                .is_empty(),
            "a cache byte-identical to its row is never set aside"
        );
    }

    /// The disclosed first-boot residual: a cache that genuinely differs from
    /// its row on a host that has never reconciled it is set aside once, and
    /// the boot after that is quiet.
    #[tokio::test]
    async fn a_first_reconciled_boot_sets_a_differing_cache_aside_exactly_once() {
        let fixture = ImportFixture::new().await;
        let workspace = analytics();
        fixture.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        fixture.install(&workspace, &source);
        fixture.seed_rows(&workspace, &source).await;
        fixture.write_manifest(&workspace, &source.name, "name: stale\n");

        assert_eq!(fixture.import().await.hydrated_caches, 1);
        assert_eq!(fixture.import().await.hydrated_caches, 0);

        assert_eq!(
            fixture.set_aside_manifests(&workspace, &source.name).len(),
            1,
            "the second boot must find the cache provably Coral's own"
        );
        assert_eq!(
            fixture.manifest_bytes(&workspace, &source.name),
            Some(manifest_yaml("reports").into_bytes())
        );
    }

    /// Re-materializing an unchanged manifest moves the fingerprint's
    /// generator versions and descriptor hash while its manifest hash stands
    /// still, which is why freshness is decided on the fingerprint's bytes.
    #[tokio::test]
    async fn a_rematerialization_at_unchanged_manifest_bytes_refreshes_the_other_host() {
        // Same length, one generator version apart: only a byte comparison
        // separates these, which is the comparison the rule is built on.
        const MATERIALIZED: &str = "projection_generator_version: 1\ndescriptor_sha256: aa\n";
        const REMATERIALIZED: &str = "projection_generator_version: 2\ndescriptor_sha256: aa\n";

        let first = ImportFixture::new().await;
        let workspace = analytics();
        first.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        first.install(&workspace, &source);
        first.write_materialization(&workspace, &source.name, Some(MATERIALIZED));
        first.import().await;
        let second = first.peer_host().await;
        second.declare_workspace(&workspace);
        second.install(&workspace, &source);
        second.write_materialization(&workspace, &source.name, Some(MATERIALIZED));
        second.import().await;

        first
            .rematerialize_elsewhere(&workspace, &source.name, REMATERIALIZED)
            .await;
        let report = second.import().await;

        assert_eq!(report.hydrated_caches, 1);
        assert_eq!(
            second.fingerprint(&workspace, &source.name),
            Some(REMATERIALIZED.to_string())
        );
        assert!(
            second
                .set_aside_manifests(&workspace, &source.name)
                .is_empty(),
            "the manifest never moved, so nothing about it is divergent"
        );
    }

    /// Deletions have to stick across hosts, and neither pass may undo one: the
    /// import skips the stale mirror entry, and the mirror pass adds only rows,
    /// of which the deleted source has none.
    #[tokio::test]
    async fn a_source_deleted_on_one_host_does_not_resurrect_from_the_other() {
        let first = ImportFixture::new().await;
        let workspace = analytics();
        first.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        first.install(&workspace, &source);
        first.import().await;
        let second = first.reconciled_peer_host(&workspace, &source).await;

        first.delete_rows_elsewhere(&workspace, &source.name).await;
        let report = second.import().await;

        assert_eq!(report.skipped_tombstoned, 1);
        assert_eq!(report.imported, 0);
        assert!(second.row(&workspace, &source.name).await.is_none());
    }

    /// Workspace deletions cascade their sources without tombstones, so the
    /// workspace ledger is the only proof the other host has that its config
    /// entry is a stale mirror rather than a new workspace.
    #[tokio::test]
    async fn a_workspace_deleted_on_one_host_does_not_resurrect_from_the_other() {
        let first = ImportFixture::new().await;
        let workspace = analytics();
        first.cut_over_workspace(&workspace).await;
        let source = imported_source("reports");
        first.install(&workspace, &source);
        first.import().await;
        let second = first.reconciled_peer_host(&workspace, &source).await;

        first.delete_workspace_elsewhere(&workspace).await;
        let report = second.import().await;

        assert_eq!(report.workspaces_skipped_deleted, 1);
        assert_eq!(report.workspaces_created, 0);
        assert!(workspace_ids(&second.db).await.is_empty());
    }

    /// What a second host gets for free, and what it does not. The catalog row
    /// and both artifact caches arrive from the database — enough to list,
    /// inspect, and load the source. Credential material does not: it is
    /// host-local by design, which is what a query against it surfaces as the
    /// missing-secret diagnostic `load_query_source` raises.
    #[tokio::test]
    async fn a_second_host_hydrates_an_imported_source_but_never_its_credentials() {
        let first = ImportFixture::new().await;
        let workspace = analytics();
        first.cut_over_workspace(&workspace).await;
        let mut source = imported_source("reports");
        source.secrets = vec!["token".to_string()];
        source.credential_storage = Some(CredentialStorageKind::File);
        first.install(&workspace, &source);
        let credential_set = CredentialSetId::for_source(&source.name);
        CredentialManager::new(CredentialStore::new(first.layout.clone()))
            .replace_material(
                &workspace,
                &credential_set,
                CredentialStorageKind::File,
                &BTreeMap::from([("token".to_string(), "first-host-secret".to_string())]),
            )
            .expect("store credential material");
        first.import().await;

        let second = first.peer_host().await;
        let report = second.import().await;

        assert_eq!(report.mirrored_entries, 1);
        assert_eq!(report.hydrated_caches, 2);
        assert_eq!(
            second.row(&workspace, &source.name).await,
            Some(source.clone()),
            "the second host lists the source from the rows of record"
        );
        assert_eq!(second.entry(&workspace, &source.name), Some(source.clone()));
        resolve_installed_manifest(&workspace, &source, &second.layout)
            .expect("the second host resolves the hydrated manifest");
        assert_eq!(
            CredentialManager::new(CredentialStore::new(second.layout.clone()))
                .read_material(&workspace, &credential_set, CredentialStorageKind::File)
                .expect("read credential material"),
            BTreeMap::new(),
            "credential material is per host and never travels with the row"
        );
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        open_sqlite_at(path).await
    }

    async fn open_sqlite_at(path: PathBuf) -> CoralDb {
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        db
    }
}
