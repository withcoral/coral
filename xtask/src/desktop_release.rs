//! Desktop release: version agreement, packaging, and the v1 update feed.

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest as _, Sha512};

use crate::release::{
    open_macos_signing, run_command, run_command_output, run_command_output_allow_failure,
};

const DESKTOP_DIR: &str = "apps/desktop";

/// The macOS app in electron-builder's output directory.
const MAC_APP: &str = "mac-arm64/Coral.app";

#[derive(Debug, clap::Args)]
pub(crate) struct CheckVersionArgs {
    /// Release tag, vX.Y.Z or vX.Y.Z-PRERELEASE.
    #[arg(long)]
    tag: String,
}

/// Fails unless the tag, the runtime and Desktop name one version.
pub(crate) fn check_version(args: &CheckVersionArgs) -> Result<bool> {
    let version = workspace_release_version(&args.tag)?;
    println!("Release {version}: the tag, the runtime and Desktop agree");
    Ok(true)
}

#[derive(Debug, clap::Args)]
pub(crate) struct PackageArgs {
    /// Platform to package on this host.
    #[arg(long, value_enum)]
    platform: DesktopPlatform,

    /// Release tag the package's version must match.
    #[arg(long)]
    tag: String,

    /// The v1 feed repository, owner/name, that the package must update from.
    #[arg(long)]
    feed_repo: String,

    /// Temporary directory for signing secrets and the keychain. macOS only.
    #[arg(long, required_if_eq("platform", "mac"))]
    work_dir: Option<PathBuf>,

    /// Directory to receive the verified release assets.
    #[arg(long)]
    out: PathBuf,
}

/// Packages the staged Desktop build for one platform, signs and notarizes
/// it where the platform requires, verifies the result, and copies the
/// release assets to `out`.
///
/// Run it after `npm run stage` and `npm run build` in apps/desktop, so that
/// nothing compiles while signing credentials are in the environment.
pub(crate) fn package(args: &PackageArgs) -> Result<bool> {
    let version = workspace_release_version(&args.tag)?;
    let dist = Path::new(DESKTOP_DIR).join("dist");
    remove_dir_if_present(&dist)?;

    match args.platform {
        DesktopPlatform::Mac => {
            package_mac(args.work_dir.as_deref().context("macOS needs --work-dir")?)?;
        }
        DesktopPlatform::Win => {
            electron_builder("--win", &[])?;
            verify_windows_signatures(&dist)?;
        }
        DesktopPlatform::Linux => electron_builder("--linux", &[])?,
    }

    let update_config = dist.join(args.platform.unpacked_update_config());
    let contents = fs::read_to_string(&update_config)
        .with_context(|| format!("reading {}", update_config.display()))?;
    verify_update_config(&contents, args.platform, &args.feed_repo)?;

    let assets = verify_feed(&dist, args.platform, &version)?;
    fs::create_dir_all(&args.out).with_context(|| format!("creating {}", args.out.display()))?;
    for asset in &assets {
        fs::copy(dist.join(asset), args.out.join(asset))
            .with_context(|| format!("copying {asset} to {}", args.out.display()))?;
    }
    println!(
        "Verified {:?} Desktop {version}: {}",
        args.platform,
        assets.join(", ")
    );
    Ok(true)
}

fn package_mac(work_dir: &Path) -> Result<()> {
    if !cfg!(target_os = "macos") {
        bail!("the macOS package must be built on macOS");
    }
    let mut signing = open_macos_signing(work_dir)?;

    electron_builder("--mac", &signing.electron_builder_env())?;
    signing.close()?;

    let app = Path::new(DESKTOP_DIR).join("dist").join(MAC_APP);
    run_command(
        Command::new("codesign")
            .args(["--verify", "--deep", "--strict", "--verbose=2"])
            .arg(&app),
        "verifying the app's signature",
    )?;
    run_command(
        Command::new("xcrun")
            .args(["stapler", "validate"])
            .arg(&app),
        "validating the notarization staple",
    )?;
    run_command(
        Command::new("spctl")
            .args(["--assess", "--type", "execute", "--verbose=2"])
            .arg(&app),
        "assessing the app as Gatekeeper does",
    )?;
    // The runtime runs Programs in V8, which aborts without JIT.
    verify_jit(&app)?;
    verify_jit(&app.join("Contents/Resources/coral/coral"))
}

/// Checks that a signed macOS binary has the hardened runtime and the JIT
/// entitlement V8 needs under it.
fn verify_jit(code: &Path) -> Result<()> {
    let details = run_command_output(
        Command::new("codesign")
            .args(["--display", "--verbose=2"])
            .arg(code),
        "reading the signature",
    )?;
    // codesign prints signature details on stderr.
    let details = String::from_utf8_lossy(&details.stderr);
    if !details.contains("(runtime)") {
        bail!("{} is not signed with the hardened runtime", code.display());
    }
    let entitlements = run_command_output(
        Command::new("codesign")
            .args(["--display", "--entitlements", "-", "--xml"])
            .arg(code),
        "reading entitlements",
    )?;
    if !String::from_utf8_lossy(&entitlements.stdout).contains("com.apple.security.cs.allow-jit") {
        bail!("{} lacks the JIT entitlement", code.display());
    }
    Ok(())
}

/// Checks the Authenticode signatures of the installer, the app and the
/// runtime it supervises.
fn verify_windows_signatures(dist: &Path) -> Result<()> {
    let platform = DesktopPlatform::Win;
    let installer = single_file(dist, platform.package_prefix(), platform.update_package())?;
    let unpacked = dist.join("win-unpacked");
    for code in [
        installer,
        unpacked.join("Coral.exe"),
        unpacked.join("resources").join("coral").join("coral.exe"),
    ] {
        let status = authenticode_status(&code)?;
        if status != "Valid" {
            bail!("{} has Authenticode status {status}", code.display());
        }
    }
    Ok(())
}

/// What Windows says about a file's Authenticode signature: `Valid`,
/// `NotSigned`, `HashMismatch` and so on.
fn authenticode_status(file: &Path) -> Result<String> {
    // -Command joins any further arguments into the script text, so the path
    // travels in the environment, where it needs no quoting.
    let status = run_command_output(
        Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command"])
            .arg("(Get-AuthenticodeSignature -LiteralPath $env:CORAL_SIGNED_FILE).Status")
            .env("CORAL_SIGNED_FILE", file)
            // Started from PowerShell 7, as CI does, Windows PowerShell would
            // inherit its module path and fail to load its own modules.
            .env_remove("PSModulePath"),
        "reading the Authenticode signature",
    )?;
    Ok(String::from_utf8_lossy(&status.stdout).trim().to_owned())
}

/// Runs electron-builder for `platform_flag` on the staged build. It never
/// publishes: the release workflow uploads only verified assets.
fn electron_builder(platform_flag: &str, env: &[(&str, OsString)]) -> Result<()> {
    let mut command = Command::new("node");
    command
        .current_dir(DESKTOP_DIR)
        .arg(
            Path::new("node_modules")
                .join("electron-builder")
                .join("cli.js"),
        )
        .args([
            "--config",
            "electron-builder.config.ts",
            platform_flag,
            "--publish",
            "never",
        ])
        .env("CORAL_DESKTOP_RELEASE", "1");
    for (name, value) in env {
        command.env(name, value);
    }
    // Streams electron-builder's progress rather than holding it to the end.
    let status = command
        .status()
        .context("starting electron-builder; run `npm ci --prefix apps/desktop` first")?;
    if !status.success() {
        bail!("electron-builder failed with {status}");
    }
    Ok(())
}

fn single_file(dir: &Path, prefix: &str, extension: &str) -> Result<PathBuf> {
    let mut matches = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if name.starts_with(prefix) && name.ends_with(extension) {
            matches.push(dir.join(name));
        }
    }
    match matches.as_slice() {
        [single] => Ok(single.clone()),
        _ => bail!(
            "expected one {prefix}*{extension} in {}, found {matches:?}",
            dir.display()
        ),
    }
}

fn remove_dir_if_present(dir: &Path) -> Result<()> {
    match fs::remove_dir_all(dir) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            Err(error).with_context(|| format!("removing {}", dir.display()))
        }
        _ => Ok(()),
    }
}

#[derive(Debug, clap::Args)]
pub(crate) struct FeedReleaseArgs {
    /// Release tag.
    #[arg(long)]
    tag: String,

    /// The v1 feed repository, owner/name.
    #[arg(long)]
    feed_repo: String,

    /// Directory holding every platform's verified release assets.
    #[arg(long)]
    assets: PathBuf,
}

#[derive(Debug, clap::Args)]
pub(crate) struct PublishArgs {
    #[command(flatten)]
    feed: FeedReleaseArgs,

    /// Markdown release notes for the draft, read when the draft is created.
    #[arg(long)]
    notes_file: PathBuf,
}

/// Uploads every platform's verified assets to a draft release in the feed
/// repository. Updaters ignore drafts, so none sees the release until
/// `promote` publishes it whole. A rerun replaces the draft's assets and
/// keeps the notes it was created with.
pub(crate) fn publish(publish_args: &PublishArgs) -> Result<bool> {
    let args = &publish_args.feed;
    let verified = verified_assets(args)?;
    match release_state(&args.tag, &args.feed_repo)? {
        Some(ReleaseState {
            is_draft: false, ..
        }) => {
            bail!("{} is already published in {}", args.tag, args.feed_repo);
        }
        Some(_) => {}
        None => {
            let mut create = Command::new("gh");
            create
                .args([
                    "release",
                    "create",
                    &args.tag,
                    "--repo",
                    &args.feed_repo,
                    "--draft",
                ])
                .args(["--title", &format!("Coral {}", args.tag)])
                .arg("--notes-file")
                .arg(&publish_args.notes_file);
            if is_prerelease(&args.tag) {
                create.arg("--prerelease");
            }
            run_command(&mut create, "creating the draft release")?;
        }
    }

    let mut upload = Command::new("gh");
    upload.args([
        "release",
        "upload",
        &args.tag,
        "--repo",
        &args.feed_repo,
        "--clobber",
    ]);
    upload.args(verified.iter().map(|asset| args.assets.join(&asset.name)));
    run_command(&mut upload, "uploading release assets")?;

    require_complete(&args.tag, &args.feed_repo, &verified)?;
    println!(
        "Drafted {} in {} with {} assets",
        args.tag,
        args.feed_repo,
        verified.len()
    );
    Ok(true)
}

/// Publishes the draft once it holds every verified asset. From then on,
/// updaters see the release.
pub(crate) fn promote(args: &FeedReleaseArgs) -> Result<bool> {
    let verified = verified_assets(args)?;
    let state = require_complete(&args.tag, &args.feed_repo, &verified)?;
    if !state.is_draft {
        println!("{} is already published in {}", args.tag, args.feed_repo);
        return Ok(true);
    }
    // Prereleases reach only builds that are prereleases themselves.
    let latest = if is_prerelease(&args.tag) {
        "--latest=false"
    } else {
        "--latest"
    };
    run_command(
        Command::new("gh")
            .args(["release", "edit", &args.tag, "--repo", &args.feed_repo])
            .args(["--draft=false", latest]),
        "publishing the release",
    )?;
    println!("Published {} in {}", args.tag, args.feed_repo);
    Ok(true)
}

/// Every platform's feed checked against the packages in `dir`, with the
/// size of each asset.
fn verified_assets(args: &FeedReleaseArgs) -> Result<Vec<Asset>> {
    let (tag, dir) = (&args.tag, &args.assets);
    let version = workspace_release_version(tag)?;
    let mut assets = Vec::new();
    for platform in DesktopPlatform::ALL {
        for name in verify_feed(dir, platform, &version)? {
            let size = fs::metadata(dir.join(&name))
                .with_context(|| format!("reading {name}"))?
                .len();
            assets.push(Asset { name, size });
        }
    }
    Ok(assets)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseState {
    is_draft: bool,
    assets: Vec<Asset>,
}

fn release_state(tag: &str, repo: &str) -> Result<Option<ReleaseState>> {
    let output = run_command_output_allow_failure(
        Command::new("gh").args([
            "release",
            "view",
            tag,
            "--repo",
            repo,
            "--json",
            "isDraft,assets",
        ]),
        "reading the release",
    )?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("release not found") {
            return Ok(None);
        }
        bail!("reading {tag} in {repo} failed: {stderr}");
    }
    serde_json::from_slice(&output.stdout)
        .map(Some)
        .context("parsing the release")
}

fn require_complete(tag: &str, repo: &str, verified: &[Asset]) -> Result<ReleaseState> {
    let state =
        release_state(tag, repo)?.with_context(|| format!("{repo} has no release {tag}"))?;
    let unmatched = unmatched_assets(verified, &state.assets);
    if !unmatched.is_empty() {
        bail!(
            "{tag} in {repo} lacks or truncates {}",
            unmatched.join(", ")
        );
    }
    Ok(state)
}

fn is_prerelease(tag: &str) -> bool {
    tag.contains('-')
}

fn workspace_release_version(tag: &str) -> Result<String> {
    let cargo = fs::read_to_string("Cargo.toml").context("reading Cargo.toml")?;
    let desktop = fs::read_to_string(Path::new(DESKTOP_DIR).join("package.json"))
        .context("reading apps/desktop/package.json")?;
    release_version(tag, &cargo, &desktop)
}

/// A platform that Desktop releases for, with the feed electron-updater reads
/// there and the package it installs from that feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum DesktopPlatform {
    Mac,
    Win,
    Linux,
}

impl DesktopPlatform {
    const ALL: [Self; 3] = [Self::Mac, Self::Win, Self::Linux];

    fn feed(self) -> &'static str {
        match self {
            Self::Mac => "latest-mac.yml",
            Self::Win => "latest.yml",
            Self::Linux => "latest-linux.yml",
        }
    }

    /// The extension of the package that the updater installs.
    fn update_package(self) -> &'static str {
        match self {
            // Squirrel.Mac installs from the zip; the dmg is for first installs.
            Self::Mac => ".zip",
            Self::Win => ".exe",
            Self::Linux => ".AppImage",
        }
    }

    /// Whether the update package has a separate blockmap for differential
    /// downloads. An `AppImage` carries its own.
    fn has_blockmap(self) -> bool {
        self != Self::Linux
    }

    /// Where electron-builder leaves `app-update.yml` in the unpacked app.
    fn unpacked_update_config(self) -> PathBuf {
        let resources = match self {
            Self::Mac => Path::new(MAC_APP).join("Contents/Resources"),
            Self::Win => PathBuf::from("win-unpacked/resources"),
            Self::Linux => PathBuf::from("linux-unpacked/resources"),
        };
        resources.join("app-update.yml")
    }

    /// The prefix of this platform's packages in electron-builder's `artifactName`.
    fn package_prefix(self) -> &'static str {
        match self {
            Self::Mac => "coral-app-mac-",
            Self::Win => "coral-app-win-",
            Self::Linux => "coral-app-linux-",
        }
    }
}

/// One electron-updater feed, such as `latest-mac.yml`.
#[derive(Debug, Deserialize)]
struct Feed {
    version: String,
    files: Vec<FeedFile>,
    /// The package that older updaters install, and the one that newer
    /// updaters fall back to.
    path: String,
}

#[derive(Debug, Deserialize)]
struct FeedFile {
    url: String,
    sha512: String,
    size: Option<u64>,
}

/// Checks `platform`'s feed in `dir` against the packages beside it, and
/// returns the release assets for that platform, sorted: the feed, the
/// platform's packages, and their blockmaps.
fn verify_feed(dir: &Path, platform: DesktopPlatform, version: &str) -> Result<Vec<String>> {
    let feed_name = platform.feed();
    let raw = fs::read_to_string(dir.join(feed_name))
        .with_context(|| format!("reading {feed_name} in {}", dir.display()))?;
    let feed: Feed = serde_yaml::from_str(&raw).with_context(|| format!("parsing {feed_name}"))?;

    if feed.version != version {
        bail!("{feed_name} names version {}, not {version}", feed.version);
    }
    let update = platform.update_package();
    if !feed.path.ends_with(update) || !feed.files.iter().any(|file| file.url == feed.path) {
        bail!(
            "{feed_name} installs {}; expected a {update} it lists",
            feed.path
        );
    }
    for file in &feed.files {
        let bytes = fs::read(dir.join(&file.url))
            .with_context(|| format!("{feed_name} lists {}, which is missing", file.url))?;
        if STANDARD.encode(Sha512::digest(&bytes)) != file.sha512 {
            bail!("{} does not match its sha512 in {feed_name}", file.url);
        }
        if file.size.is_some_and(|size| size != bytes.len() as u64) {
            bail!("{} does not match its size in {feed_name}", file.url);
        }
    }
    let blockmap = format!("{}.blockmap", feed.path);
    if platform.has_blockmap() && !dir.join(&blockmap).is_file() {
        bail!("{blockmap} is missing; differential updates need the blockmap");
    }

    let mut assets = vec![feed_name.to_owned()];
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type()?.is_file() && name.starts_with(platform.package_prefix()) {
            assets.push(name);
        }
    }
    assets.sort();
    Ok(assets)
}

/// Returns the release version once the tag, the Rust workspace and Desktop
/// all name it. release-please moves them together; a tag cut by hand on a
/// commit it never bumped would publish a feed that names the wrong version.
fn release_version(tag: &str, cargo_toml: &str, package_json: &str) -> Result<String> {
    let cargo: toml::Table = toml::from_str(cargo_toml).context("parsing Cargo.toml")?;
    let runtime = cargo
        .get("workspace")
        .and_then(|workspace| workspace.get("package"))
        .and_then(|package| package.get("version"))
        .and_then(toml::Value::as_str)
        .context("Cargo.toml has no [workspace.package] version")?;
    let desktop: Value =
        serde_json::from_str(package_json).context("parsing apps/desktop/package.json")?;
    let desktop = desktop
        .get("version")
        .and_then(Value::as_str)
        .context("apps/desktop/package.json has no version")?;

    let Some(version) = tag.strip_prefix('v') else {
        bail!("release tag {tag} must start with v");
    };
    if runtime != version || desktop != version {
        bail!(
            "release tag {tag} needs runtime and Desktop at {version}; they are {runtime} and {desktop}"
        );
    }
    Ok(version.to_owned())
}

/// The 0.x app polls this repository's releases. A v1 feed there would
/// update that app into v1.
const DEPRECATED_FEED: &str = "withcoral/coral";

/// Where the updater keeps downloads. electron-builder names it after the
/// package name, and the 0.x app's package was `@withcoral/desktop`.
const UPDATER_CACHE: &str = "coral-app-updater";

/// `app-update.yml`, which electron-builder packs into a release app from its
/// publish configuration.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateConfig {
    provider: String,
    owner: Option<String>,
    repo: Option<String>,
    updater_cache_dir_name: Option<String>,
    #[serde(default)]
    publisher_name: Vec<String>,
}

/// Checks that a packaged app's `app-update.yml` follows `feed_repo`, the v1
/// feed, and on Windows that the updater can check the installer's signature.
fn verify_update_config(contents: &str, platform: DesktopPlatform, feed_repo: &str) -> Result<()> {
    if feed_repo == DEPRECATED_FEED {
        bail!("{feed_repo} is the 0.x app's feed; v1 needs a feed of its own");
    }
    let config: UpdateConfig = serde_yaml::from_str(contents).context("parsing app-update.yml")?;
    let source = format!(
        "{}/{}",
        config.owner.as_deref().unwrap_or_default(),
        config.repo.as_deref().unwrap_or_default()
    );
    if config.provider != "github" || source != feed_repo {
        bail!("the app updates from {source}, not {feed_repo}");
    }
    if config.updater_cache_dir_name.as_deref() != Some(UPDATER_CACHE) {
        bail!(
            "the app keeps updates in {:?}, not {UPDATER_CACHE}; it could share the 0.x app's updater cache",
            config.updater_cache_dir_name
        );
    }
    // NsisUpdater checks the installer's Authenticode signer against this
    // list, and skips the check when it is empty.
    if platform == DesktopPlatform::Win && config.publisher_name.is_empty() {
        bail!("app-update.yml names no publisherName; the updater would not check signatures");
    }
    Ok(())
}

/// A release asset by name and size, as GitHub lists it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct Asset {
    name: String,
    size: u64,
}

/// The verified assets that `uploaded` lacks, or holds at another size.
fn unmatched_assets(verified: &[Asset], uploaded: &[Asset]) -> Vec<String> {
    verified
        .iter()
        .filter(|asset| !uploaded.contains(asset))
        .map(|asset| asset.name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use tempfile::TempDir;

    use super::{
        Asset, DesktopPlatform, release_version, unmatched_assets, verify_feed,
        verify_update_config,
    };

    // SHA-512 digests, in base64, of the fixture packages' contents.
    const ZIP_SHA512: &str =
        "A5JY2soDJJhmgGhmPv6GEdDq+HBY/XClPIYl3ihyBDSzsovWx0Zygh1Ia3qqCnnyhsAwSsPI8qr1BFKbpXR0tA==";
    const DMG_SHA512: &str =
        "gKx0owVaTtVJlW7qI1bvj0oBsn8xrap/0BIz0klLjEGj12pTrV38CwKJU/jpv/F06AWFSYlv0ulTu9Kx7gDw3w==";

    /// A macOS dist directory as electron-builder writes it, before any change.
    fn mac_dist() -> TempDir {
        let dist = TempDir::new().expect("temp dir");
        let write = |name: &str, contents: &str| {
            fs::write(dist.path().join(name), contents).expect("write fixture");
        };
        write("coral-app-mac-arm64.zip", "zip");
        write("coral-app-mac-arm64.zip.blockmap", "blockmap");
        write("coral-app-mac-arm64.dmg", "dmg");
        write("coral-app-mac-arm64.dmg.blockmap", "blockmap");
        write("builder-debug.yml", "not a release asset");
        write(
            "latest-mac.yml",
            &format!(
                "version: 1.2.0\nfiles:\n  - url: coral-app-mac-arm64.zip\n    sha512: {ZIP_SHA512}\n    size: 3\n  - url: coral-app-mac-arm64.dmg\n    sha512: {DMG_SHA512}\n    size: 3\npath: coral-app-mac-arm64.zip\nsha512: {ZIP_SHA512}\nreleaseDate: '2026-10-03T00:00:00.000Z'\n"
            ),
        );
        dist
    }

    fn edit(dist: &Path, name: &str, from: &str, to: &str) {
        let path = dist.join(name);
        let contents = fs::read_to_string(&path).expect("read fixture");
        assert!(contents.contains(from), "{name} does not contain {from}");
        fs::write(path, contents.replace(from, to)).expect("write fixture");
    }

    #[test]
    fn verify_feed_lists_the_release_assets_of_a_complete_dist() {
        let dist = mac_dist();

        let assets =
            verify_feed(dist.path(), DesktopPlatform::Mac, "1.2.0").expect("complete dist");

        assert_eq!(
            assets,
            [
                "coral-app-mac-arm64.dmg",
                "coral-app-mac-arm64.dmg.blockmap",
                "coral-app-mac-arm64.zip",
                "coral-app-mac-arm64.zip.blockmap",
                "latest-mac.yml",
            ]
        );
    }

    #[test]
    fn verify_feed_rejects_a_feed_the_updater_cannot_trust() {
        type Break = fn(&Path);
        let cases: [(&str, Break, &str); 6] = [
            (
                "another version",
                |dist| edit(dist, "latest-mac.yml", "version: 1.2.0", "version: 1.1.0"),
                "names version 1.1.0",
            ),
            (
                "a changed package",
                |dist| fs::write(dist.join("coral-app-mac-arm64.zip"), "zap").expect("write"),
                "sha512",
            ),
            (
                "a missing package",
                |dist| fs::remove_file(dist.join("coral-app-mac-arm64.dmg")).expect("remove"),
                "coral-app-mac-arm64.dmg",
            ),
            (
                "the dmg as the update",
                |dist| {
                    edit(
                        dist,
                        "latest-mac.yml",
                        "path: coral-app-mac-arm64.zip",
                        "path: coral-app-mac-arm64.dmg",
                    );
                },
                "expected a .zip",
            ),
            (
                "no blockmap",
                |dist| {
                    fs::remove_file(dist.join("coral-app-mac-arm64.zip.blockmap")).expect("remove");
                },
                "blockmap",
            ),
            (
                "no feed",
                |dist| fs::remove_file(dist.join("latest-mac.yml")).expect("remove"),
                "latest-mac.yml",
            ),
        ];
        for (case, break_dist, message) in cases {
            let dist = mac_dist();
            break_dist(dist.path());

            let error = verify_feed(dist.path(), DesktopPlatform::Mac, "1.2.0")
                .expect_err(case)
                .to_string();

            assert!(error.contains(message), "{case}: {error}");
        }
    }

    #[test]
    fn verify_feed_accepts_an_appimage_without_a_separate_blockmap() {
        let dist = TempDir::new().expect("temp dir");
        fs::write(
            dist.path().join("coral-app-linux-x86_64.AppImage"),
            "AppImage",
        )
        .expect("write");
        fs::write(
            dist.path().join("latest-linux.yml"),
            "version: 1.2.0\nfiles:\n  - url: coral-app-linux-x86_64.AppImage\n    sha512: CMKX4VS5oFHj2lS3AdtW9pBi4Qhzv+06UUafloJ7Gs4CD9fW1Z9mo2ymCHpcpfS8TAtDWNndIgW+IdstyFwgGA==\n    size: 8\npath: coral-app-linux-x86_64.AppImage\n",
        )
        .expect("write");

        let assets =
            verify_feed(dist.path(), DesktopPlatform::Linux, "1.2.0").expect("complete dist");

        assert_eq!(
            assets,
            ["coral-app-linux-x86_64.AppImage", "latest-linux.yml"]
        );
    }

    #[test]
    fn verify_update_config_requires_the_v1_feed() {
        let feed = "withcoral/coral-releases";
        let config = |repo: &str, publisher: &str| {
            format!(
                "owner: withcoral\nrepo: {repo}\nprovider: github\nupdaterCacheDirName: coral-app-updater\n{publisher}"
            )
        };
        let signed = "publisherName:\n  - With Coral Ltd\n";
        let cases = [
            (
                DesktopPlatform::Mac,
                config("coral-releases", ""),
                feed,
                Ok(()),
            ),
            (
                DesktopPlatform::Win,
                config("coral-releases", signed),
                feed,
                Ok(()),
            ),
            (
                DesktopPlatform::Mac,
                config("coral", ""),
                feed,
                Err("updates from withcoral/coral, not withcoral/coral-releases"),
            ),
            (
                DesktopPlatform::Win,
                config("coral-releases", ""),
                feed,
                Err("publisherName"),
            ),
            (
                DesktopPlatform::Linux,
                config("coral", ""),
                "withcoral/coral",
                Err("the 0.x app's feed"),
            ),
            (
                DesktopPlatform::Linux,
                config("coral-releases", "")
                    .replace("coral-app-updater", "'@withcoraldesktop-updater'"),
                feed,
                Err("the 0.x app's updater cache"),
            ),
        ];
        for (platform, contents, feed, expected) in cases {
            let actual = verify_update_config(&contents, platform, feed);
            match (expected, actual) {
                (Ok(()), Ok(())) => {}
                (Err(message), Err(error)) => {
                    assert!(error.to_string().contains(message), "{platform:?}: {error}");
                }
                (expected, actual) => panic!("{platform:?}: expected {expected:?}, got {actual:?}"),
            }
        }
    }

    // Runs the real PowerShell check, so only Windows can run it. It guards
    // how the path reaches PowerShell: a path with a space once parsed as code.
    #[cfg(windows)]
    #[test]
    fn authenticode_status_reads_signed_and_unsigned_files() {
        let signed = Path::new(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe");
        assert_eq!(super::authenticode_status(signed).expect("signed"), "Valid");

        let dir = TempDir::new().expect("temp dir");
        let unsigned = dir.path().join("not signed's file.exe");
        fs::write(&unsigned, "not a program").expect("write");
        // Windows calls a file that is not a program `UnknownError`.
        assert_ne!(
            super::authenticode_status(&unsigned).expect("unsigned"),
            "Valid"
        );
    }

    #[test]
    fn unmatched_assets_names_each_asset_the_draft_lacks_or_holds_truncated() {
        let asset = |name: &str, size| Asset {
            name: name.to_owned(),
            size,
        };
        let verified = [
            asset("latest-mac.yml", 10),
            asset("coral-app-mac-arm64.zip", 300),
            asset("coral-app-win-x64.exe", 400),
        ];
        let uploaded = [
            asset("latest-mac.yml", 10),
            asset("coral-app-mac-arm64.zip", 120),
            asset("notes.txt", 1),
        ];

        assert_eq!(
            unmatched_assets(&verified, &uploaded),
            ["coral-app-mac-arm64.zip", "coral-app-win-x64.exe"]
        );
        assert!(unmatched_assets(&verified, &verified).is_empty());
    }

    fn cargo(version: &str) -> String {
        format!("[workspace.package]\nedition = \"2024\"\nversion = \"{version}\"\n")
    }

    fn desktop(version: &str) -> String {
        format!(r#"{{"name": "@withcoral/desktop", "version": "{version}"}}"#)
    }

    #[test]
    fn release_version_requires_tag_runtime_and_desktop_to_agree() {
        let cases = [
            ("v1.2.0", "1.2.0", "1.2.0", Ok("1.2.0")),
            (
                "v1.2.0-beta.1",
                "1.2.0-beta.1",
                "1.2.0-beta.1",
                Ok("1.2.0-beta.1"),
            ),
            ("1.2.0", "1.2.0", "1.2.0", Err("must start with v")),
            ("v1.2.0", "1.1.0", "1.2.0", Err("they are 1.1.0 and 1.2.0")),
            ("v1.2.0", "1.2.0", "1.1.0", Err("they are 1.2.0 and 1.1.0")),
        ];
        for (tag, runtime, app, expected) in cases {
            let actual = release_version(tag, &cargo(runtime), &desktop(app));
            match (expected, actual) {
                (Ok(version), Ok(actual)) => assert_eq!(actual, version, "{tag}"),
                (Err(message), Err(error)) => {
                    assert!(error.to_string().contains(message), "{tag}: {error}");
                }
                (expected, actual) => panic!("{tag}: expected {expected:?}, got {actual:?}"),
            }
        }
    }
}
