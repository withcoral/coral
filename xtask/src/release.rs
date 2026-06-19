//! Release workflow helpers.

use std::ffi::OsString;
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;
use uuid::Uuid;

use crate::env;

const DEVELOPER_ID_G2_CA_URL: &str =
    "https://www.apple.com/certificateauthority/DeveloperIDG2CA.cer";
const DEVELOPER_ID_G2_CA_SHA256: &str =
    "F16CD3C54C7F83CEA4BF1A3E6A0819C8AAA8E4A1528FD144715F350643D2DF3A";

#[derive(Debug, clap::Args)]
pub(crate) struct MacosSignNotarizeArgs {
    /// macOS Rust target being packaged, for diagnostics only.
    #[arg(long)]
    target: String,

    /// Built, unsigned Coral binary to sign in place.
    #[arg(long)]
    binary: PathBuf,

    /// Destination ZIP archive to submit for notarization.
    #[arg(long)]
    zip: PathBuf,

    /// Destination legacy tar.gz archive.
    #[arg(long)]
    tar: PathBuf,

    /// Temporary directory for signing assets and keychain state.
    #[arg(long)]
    work_dir: PathBuf,

    /// Code signing identifier embedded in the binary signature.
    #[arg(long, default_value = "com.withcoral.coral")]
    identifier: String,
}

pub(crate) fn macos_sign_notarize(args: &MacosSignNotarizeArgs) -> Result<bool> {
    if !cfg!(target_os = "macos") {
        bail!("release-macos-sign-notarize must run on macOS");
    }
    if !args.target.ends_with("-apple-darwin") {
        bail!("target must be a macOS target, got '{}'", args.target);
    }
    if !args.binary.is_file() {
        bail!("binary does not exist: {}", args.binary.display());
    }

    let env = SigningEnv::read()?;
    let mut session = SigningSession::create(&args.work_dir)?;

    let certificate_path = session.work_dir.join("developer_id_application.p12");
    let key_path = session
        .work_dir
        .join(format!("AuthKey_{}.p8", env.app_store_connect_api_key_id));
    let developer_id_ca_path = session.work_dir.join("DeveloperIDG2CA.cer");

    write_base64_secret(
        "APPLE_DEVELOPER_ID_CERTIFICATE_BASE64",
        &env.apple_developer_id_certificate_base64,
        &certificate_path,
    )?;
    write_base64_secret(
        "APP_STORE_CONNECT_API_KEY_P8_BASE64",
        &env.app_store_connect_api_key_p8_base64,
        &key_path,
    )?;

    install_developer_id_g2_ca(&developer_id_ca_path)?;
    session.create_keychain()?;
    import_signing_identity(
        &certificate_path,
        &env.apple_developer_id_certificate_password,
        &session.keychain_path,
    )?;
    allow_codesign_key_access(&session.keychain_path, &session.keychain_password)?;
    session.prepend_keychain_to_search_list()?;
    find_signing_identity(&session.keychain_path)?;

    sign_binary(&args.binary, &env.apple_codesign_identity, &args.identifier)?;
    let zip = output_path(&args.zip)?;
    let tar = output_path(&args.tar)?;
    package_archives(&args.binary, &zip, &tar, &session.work_dir)?;
    submit_and_verify_notarization(
        &zip,
        &args.binary,
        &key_path,
        &env.app_store_connect_api_key_id,
        &env.app_store_connect_api_issuer_id,
        &session.work_dir,
    )?;

    session.cleanup()?;
    Ok(true)
}

#[derive(Debug)]
struct SigningEnv {
    app_store_connect_api_issuer_id: String,
    app_store_connect_api_key_id: String,
    app_store_connect_api_key_p8_base64: String,
    apple_codesign_identity: String,
    apple_developer_id_certificate_base64: String,
    apple_developer_id_certificate_password: String,
}

impl SigningEnv {
    fn read() -> Result<Self> {
        Ok(Self {
            app_store_connect_api_issuer_id: required_env("APP_STORE_CONNECT_API_ISSUER_ID")?,
            app_store_connect_api_key_id: required_env("APP_STORE_CONNECT_API_KEY_ID")?,
            app_store_connect_api_key_p8_base64: required_env(
                "APP_STORE_CONNECT_API_KEY_P8_BASE64",
            )?,
            apple_codesign_identity: required_env("APPLE_CODESIGN_IDENTITY")?,
            apple_developer_id_certificate_base64: required_env(
                "APPLE_DEVELOPER_ID_CERTIFICATE_BASE64",
            )?,
            apple_developer_id_certificate_password: required_env(
                "APPLE_DEVELOPER_ID_CERTIFICATE_PASSWORD",
            )?,
        })
    }
}

fn required_env(name: &str) -> Result<String> {
    env::required_var(name)
}

#[derive(Debug)]
struct SigningSession {
    work_dir: PathBuf,
    keychain_path: PathBuf,
    keychain_password: String,
    original_keychains: Vec<PathBuf>,
    cleaned: bool,
}

impl SigningSession {
    fn create(work_dir: &Path) -> Result<Self> {
        fs::create_dir_all(work_dir).with_context(|| format!("creating {}", work_dir.display()))?;

        let keychain_path = work_dir.join("coral-signing.keychain-db");
        let original_keychains_path = work_dir.join("original-keychains.txt");
        let original_keychains = current_user_keychains()?;
        fs::write(
            &original_keychains_path,
            format!(
                "{}{}",
                original_keychains
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join("\n"),
                if original_keychains.is_empty() {
                    ""
                } else {
                    "\n"
                }
            ),
        )
        .with_context(|| format!("writing {}", original_keychains_path.display()))?;

        let session = Self {
            work_dir: work_dir.to_path_buf(),
            keychain_path,
            keychain_password: format!("coral-{}", Uuid::new_v4()),
            original_keychains,
            cleaned: false,
        };
        session.delete_keychain_if_present();
        Ok(session)
    }

    fn create_keychain(&self) -> Result<()> {
        run_command(
            Command::new("security")
                .arg("create-keychain")
                .arg("-p")
                .arg(&self.keychain_password)
                .arg(&self.keychain_path),
            "creating signing keychain",
        )?;
        run_command(
            Command::new("security")
                .arg("set-keychain-settings")
                .arg("-lut")
                .arg("21600")
                .arg(&self.keychain_path),
            "configuring signing keychain",
        )?;
        run_command(
            Command::new("security")
                .arg("unlock-keychain")
                .arg("-p")
                .arg(&self.keychain_password)
                .arg(&self.keychain_path),
            "unlocking signing keychain",
        )
    }

    fn prepend_keychain_to_search_list(&self) -> Result<()> {
        let mut keychains = Vec::with_capacity(self.original_keychains.len() + 1);
        keychains.push(self.keychain_path.clone());
        keychains.extend(self.original_keychains.iter().cloned());
        set_user_keychains(&keychains)
    }

    fn cleanup(&mut self) -> Result<()> {
        if self.cleaned {
            return Ok(());
        }

        let restore_result = if self.original_keychains.is_empty() {
            Ok(())
        } else {
            set_user_keychains(&self.original_keychains)
        };
        self.delete_keychain_if_present();
        let remove_result = fs::remove_dir_all(&self.work_dir)
            .or_else(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(error)
                }
            })
            .with_context(|| format!("removing {}", self.work_dir.display()));

        self.cleaned = true;
        restore_result?;
        remove_result
    }

    fn delete_keychain_if_present(&self) {
        drop(
            Command::new("security")
                .arg("delete-keychain")
                .arg(&self.keychain_path)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status(),
        );
    }
}

impl Drop for SigningSession {
    fn drop(&mut self) {
        drop(self.cleanup());
    }
}

fn current_user_keychains() -> Result<Vec<PathBuf>> {
    let output = run_command_output(
        Command::new("security")
            .arg("list-keychains")
            .arg("-d")
            .arg("user"),
        "listing user keychains",
    )?;
    let raw = String::from_utf8(output.stdout).context("security output was not UTF-8")?;
    Ok(parse_keychain_list(&raw)
        .into_iter()
        .map(PathBuf::from)
        .collect())
}

fn parse_keychain_list(raw: &str) -> Vec<String> {
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| line.trim_matches('"').to_string())
        .collect()
}

fn set_user_keychains(keychains: &[PathBuf]) -> Result<()> {
    let mut command = Command::new("security");
    command
        .arg("list-keychains")
        .arg("-d")
        .arg("user")
        .arg("-s");
    command.args(keychains);
    run_command(&mut command, "setting user keychain search list")
}

fn write_base64_secret(env_name: &str, value: &str, path: &Path) -> Result<()> {
    let normalized: String = value.split_whitespace().collect();
    if normalized.is_empty() {
        bail!("{env_name} is empty");
    }
    let decoded = STANDARD
        .decode(normalized.as_bytes())
        .with_context(|| format!("decoding {env_name}"))?;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    file.write_all(&decoded)
        .with_context(|| format!("writing {}", path.display()))?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restricting {}", path.display()))?;
    Ok(())
}

fn install_developer_id_g2_ca(path: &Path) -> Result<()> {
    run_command(
        Command::new("curl")
            .arg("--fail")
            .arg("--location")
            .arg("--retry")
            .arg("3")
            .arg("--output")
            .arg(path)
            .arg(DEVELOPER_ID_G2_CA_URL),
        "downloading Apple Developer ID G2 certificate",
    )?;

    let status = Command::new("security")
        .arg("import")
        .arg(path)
        .arg("-k")
        .arg(login_keychain_path()?)
        .status()
        .context("importing Apple Developer ID G2 certificate")?;
    if developer_id_g2_ca_is_installed()? {
        Ok(())
    } else {
        bail!("importing Apple Developer ID G2 certificate failed with {status}")
    }
}

fn developer_id_g2_ca_is_installed() -> Result<bool> {
    let output = run_command_output_allow_failure(
        Command::new("security")
            .arg("find-certificate")
            .arg("-Z")
            .arg("-c")
            .arg("Developer ID Certification Authority")
            .arg(login_keychain_path()?),
        "checking Apple Developer ID G2 certificate",
    )?;
    if !output.status.success() {
        return Ok(false);
    }
    let stdout = String::from_utf8(output.stdout).context("security output was not UTF-8")?;
    Ok(stdout.contains(DEVELOPER_ID_G2_CA_SHA256))
}

fn login_keychain_path() -> Result<PathBuf> {
    Ok(env::home_dir()?
        .join("Library")
        .join("Keychains")
        .join("login.keychain-db"))
}

fn import_signing_identity(
    certificate_path: &Path,
    certificate_password: &str,
    keychain_path: &Path,
) -> Result<()> {
    run_command(
        Command::new("security")
            .arg("import")
            .arg(certificate_path)
            .arg("-P")
            .arg(certificate_password)
            .arg("-f")
            .arg("pkcs12")
            .arg("-T")
            .arg("/usr/bin/codesign")
            .arg("-k")
            .arg(keychain_path),
        "importing Developer ID signing identity",
    )
}

fn allow_codesign_key_access(keychain_path: &Path, keychain_password: &str) -> Result<()> {
    run_command(
        Command::new("security")
            .arg("set-key-partition-list")
            .arg("-S")
            .arg("apple-tool:,apple:,codesign:")
            .arg("-s")
            .arg("-k")
            .arg(keychain_password)
            .arg(keychain_path),
        "granting codesign keychain access",
    )
}

fn find_signing_identity(keychain_path: &Path) -> Result<()> {
    run_command(
        Command::new("security")
            .arg("find-identity")
            .arg("-p")
            .arg("codesigning")
            .arg("-v")
            .arg(keychain_path),
        "verifying signing identity",
    )
}

fn sign_binary(binary: &Path, identity: &str, identifier: &str) -> Result<()> {
    make_executable(binary)?;
    run_command(
        Command::new("codesign")
            .arg("--force")
            .arg("--sign")
            .arg(identity)
            .arg("--timestamp")
            .arg("--options")
            .arg("runtime")
            .arg("--identifier")
            .arg(identifier)
            .arg(binary),
        "signing macOS binary",
    )?;
    run_command(
        Command::new("codesign")
            .arg("--verify")
            .arg("--strict")
            .arg("--verbose=2")
            .arg(binary),
        "verifying macOS signature",
    )?;
    run_command(
        Command::new("codesign")
            .arg("-dv")
            .arg("--verbose=4")
            .arg(binary),
        "displaying macOS signature",
    )
}

fn make_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(path)
            .with_context(|| format!("reading metadata for {}", path.display()))?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)
            .with_context(|| format!("marking {} executable", path.display()))?;
    }
    Ok(())
}

fn package_archives(binary: &Path, zip: &Path, tar: &Path, work_dir: &Path) -> Result<()> {
    create_parent_dir(zip)?;
    create_parent_dir(tar)?;

    let staging = work_dir.join("package");
    fs::create_dir_all(&staging).with_context(|| format!("creating {}", staging.display()))?;
    let staged_binary = staging.join("coral");
    fs::copy(binary, &staged_binary).with_context(|| {
        format!(
            "copying signed binary from {} to {}",
            binary.display(),
            staged_binary.display()
        )
    })?;
    make_executable(&staged_binary)?;

    let mut zip_command = Command::new("ditto");
    zip_command.current_dir(&staging).args(macos_zip_args(zip));
    run_command(&mut zip_command, "creating macOS ZIP archive")?;

    run_command(
        Command::new("tar")
            .current_dir(&staging)
            .arg("czf")
            .arg(tar)
            .arg("coral"),
        "creating legacy macOS tar.gz archive",
    )
}

fn create_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    Ok(())
}

fn output_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .context("resolving current directory")?
        .join(path))
}

fn macos_zip_args(zip: &Path) -> Vec<OsString> {
    vec![
        OsString::from("-c"),
        OsString::from("-k"),
        OsString::from("coral"),
        zip.as_os_str().to_os_string(),
    ]
}

fn submit_and_verify_notarization(
    archive: &Path,
    binary: &Path,
    key_path: &Path,
    key_id: &str,
    issuer_id: &str,
    work_dir: &Path,
) -> Result<()> {
    let notary_result_path = work_dir.join("notary-result.json");
    let output = run_command_output_allow_failure(
        Command::new("xcrun")
            .arg("notarytool")
            .arg("submit")
            .arg(archive)
            .arg("--key")
            .arg(key_path)
            .arg("--key-id")
            .arg(key_id)
            .arg("--issuer")
            .arg(issuer_id)
            .arg("--wait")
            .arg("--output-format")
            .arg("json"),
        "submitting macOS ZIP for notarization",
    )?;
    fs::write(&notary_result_path, &output.stdout)
        .with_context(|| format!("writing {}", notary_result_path.display()))?;
    print_output(&output);

    let submission = parse_notary_submission(&output.stdout)
        .with_context(|| format!("parsing {}", notary_result_path.display()))?;
    if !output.status.success() {
        fetch_notary_log_if_possible(&submission, key_path, key_id, issuer_id, work_dir)?;
        bail!(
            "notarytool submit failed with {}; status={:?}",
            output.status,
            submission.status
        );
    }
    if submission.status.as_deref() != Some("Accepted") {
        fetch_notary_log_if_possible(&submission, key_path, key_id, issuer_id, work_dir)?;
        bail!(
            "notarization did not finish Accepted; id={:?} status={:?}",
            submission.id,
            submission.status
        );
    }

    run_command(
        Command::new("codesign")
            .arg("-vvvv")
            .arg("-R=notarized")
            .arg("--check-notarization")
            .arg(binary),
        "verifying notarization ticket",
    )
}

#[derive(Debug, Eq, PartialEq)]
struct NotarySubmission {
    id: Option<String>,
    status: Option<String>,
}

fn parse_notary_submission(raw: &[u8]) -> Result<NotarySubmission> {
    let value: Value = serde_json::from_slice(raw).context("parsing notarytool JSON")?;
    Ok(NotarySubmission {
        id: value
            .get("id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn fetch_notary_log_if_possible(
    submission: &NotarySubmission,
    key_path: &Path,
    key_id: &str,
    issuer_id: &str,
    work_dir: &Path,
) -> Result<()> {
    let Some(submission_id) = &submission.id else {
        return Ok(());
    };
    let notary_log_path = work_dir.join("notary-log.json");
    let output = run_command_output_allow_failure(
        Command::new("xcrun")
            .arg("notarytool")
            .arg("log")
            .arg("--key")
            .arg(key_path)
            .arg("--key-id")
            .arg(key_id)
            .arg("--issuer")
            .arg(issuer_id)
            .arg(submission_id)
            .arg(&notary_log_path),
        "fetching notarization log",
    )?;
    print_output(&output);
    if notary_log_path.is_file() {
        let log = fs::read_to_string(&notary_log_path)
            .with_context(|| format!("reading {}", notary_log_path.display()))?;
        println!("{log}");
    }
    Ok(())
}

fn run_command(command: &mut Command, label: &str) -> Result<()> {
    let output = run_command_output_allow_failure(command, label)?;
    print_output(&output);
    if !output.status.success() {
        bail!("{label} failed with {}", output.status);
    }
    Ok(())
}

fn run_command_output(command: &mut Command, label: &str) -> Result<Output> {
    let output = run_command_output_allow_failure(command, label)?;
    if !output.status.success() {
        print_output(&output);
        bail!("{label} failed with {}", output.status);
    }
    Ok(output)
}

fn run_command_output_allow_failure(command: &mut Command, label: &str) -> Result<Output> {
    command
        .output()
        .with_context(|| format!("{label}: failed to start {}", display_program(command)))
}

fn display_program(command: &Command) -> String {
    command.get_program().to_string_lossy().into_owned()
}

fn print_output(output: &Output) {
    if !output.stdout.is_empty() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
    }
    if !output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};
    use std::fs;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        NotarySubmission, macos_zip_args, output_path, parse_keychain_list,
        parse_notary_submission, write_base64_secret,
    };

    #[test]
    fn parse_keychain_list_trims_security_output() {
        let parsed = parse_keychain_list(
            r#"
                "/Users/test/Library/Keychains/login.keychain-db"
                "/tmp/coral-signing.keychain-db"
            "#,
        );
        assert_eq!(
            parsed,
            vec![
                "/Users/test/Library/Keychains/login.keychain-db",
                "/tmp/coral-signing.keychain-db"
            ]
        );
    }

    #[test]
    fn parse_notary_submission_extracts_status() {
        let parsed = parse_notary_submission(
            br#"{"id":"5555aa62-22c0-4d53-af6e-f487beef695d","status":"Accepted"}"#,
        )
        .expect("parse notary JSON");
        assert_eq!(
            parsed,
            NotarySubmission {
                id: Some("5555aa62-22c0-4d53-af6e-f487beef695d".to_string()),
                status: Some("Accepted".to_string())
            }
        );
    }

    #[test]
    fn zip_archive_keeps_binary_at_root() {
        let args = macos_zip_args(Path::new("coral-aarch64-apple-darwin.zip"));
        assert_eq!(
            args.first().map(OsString::as_os_str),
            Some(OsStr::new("-c"))
        );
        assert_eq!(args.get(1).map(OsString::as_os_str), Some(OsStr::new("-k")));
        assert_eq!(
            args.get(2).map(OsString::as_os_str),
            Some(OsStr::new("coral"))
        );
        assert!(
            !args.iter().any(|arg| arg == OsStr::new("--keepParent")),
            "zip creation must not nest coral under the staging directory"
        );
    }

    #[test]
    fn output_path_resolves_relative_paths_from_process_cwd() {
        let cwd = std::env::current_dir().expect("current directory");
        assert_eq!(
            output_path(Path::new("coral-aarch64-apple-darwin.zip")).expect("resolve output path"),
            cwd.join("coral-aarch64-apple-darwin.zip")
        );
        assert_eq!(
            output_path(Path::new("/tmp/coral-aarch64-apple-darwin.zip"))
                .expect("resolve absolute output path"),
            Path::new("/tmp/coral-aarch64-apple-darwin.zip")
        );
    }

    #[test]
    fn write_base64_secret_rejects_whitespace_only_values() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "coral-empty-base64-secret-{}-{unique}",
            std::process::id()
        ));

        let error =
            write_base64_secret("TEST_SECRET", " \n\t ", &path).expect_err("reject empty secret");

        assert_eq!(error.to_string(), "TEST_SECRET is empty");
        assert!(
            fs::metadata(&path).is_err(),
            "empty secrets must fail before creating {}",
            path.display()
        );
    }
}
