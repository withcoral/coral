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
use uuid::Uuid;

use crate::env;

const DEVELOPER_ID_G2_CA_URL: &str =
    "https://www.apple.com/certificateauthority/DeveloperIDG2CA.cer";
const DEVELOPER_ID_G2_CA_SHA256: &str =
    "F16CD3C54C7F83CEA4BF1A3E6A0819C8AAA8E4A1528FD144715F350643D2DF3A";

/// A Developer ID identity in a keychain of its own, and the App Store
/// Connect key that notarizes with it. Dropping it deletes the keychain and
/// the decoded secrets.
#[derive(Debug)]
pub(crate) struct MacosSigning {
    env: SigningEnv,
    session: SigningSession,
    /// The decoded App Store Connect API key (.p8).
    key_path: PathBuf,
}

impl MacosSigning {
    /// The environment that has electron-builder sign with this identity and
    /// notarize with this key.
    pub(crate) fn electron_builder_env(&self) -> Vec<(&'static str, OsString)> {
        vec![
            (
                "CSC_KEYCHAIN",
                self.session.keychain_path.clone().into_os_string(),
            ),
            (
                "CSC_NAME",
                electron_builder_identity(&self.env.apple_codesign_identity).into(),
            ),
            ("APPLE_API_KEY", self.key_path.clone().into_os_string()),
            (
                "APPLE_API_KEY_ID",
                self.env.app_store_connect_api_key_id.clone().into(),
            ),
            (
                "APPLE_API_ISSUER",
                self.env.app_store_connect_api_issuer_id.clone().into(),
            ),
        ]
    }

    /// Deletes the keychain and the decoded secrets.
    pub(crate) fn close(&mut self) -> Result<()> {
        self.session.cleanup()
    }
}

/// Decodes the Apple secrets into `work_dir` and imports the signing identity
/// into a temporary keychain at the front of the search list.
pub(crate) fn open_macos_signing(work_dir: &Path) -> Result<MacosSigning> {
    let env = SigningEnv::read()?;
    let session = SigningSession::create(work_dir)?;

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

    Ok(MacosSigning {
        env,
        session,
        key_path,
    })
}

/// The identity as electron-builder accepts it: without the certificate
/// type, which it refuses and finds by itself.
fn electron_builder_identity(identity: &str) -> &str {
    identity
        .strip_prefix("Developer ID Application:")
        .map_or(identity, str::trim_start)
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
    let value = env::required_var(name)?;
    // A missing GitHub secret arrives as an empty string.
    if value.trim().is_empty() {
        bail!("{name} is empty");
    }
    Ok(value)
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

pub(crate) fn run_command(command: &mut Command, label: &str) -> Result<()> {
    let output = run_command_output_allow_failure(command, label)?;
    print_output(&output);
    if !output.status.success() {
        bail!("{label} failed with {}", output.status);
    }
    Ok(())
}

pub(crate) fn run_command_output(command: &mut Command, label: &str) -> Result<Output> {
    let output = run_command_output_allow_failure(command, label)?;
    if !output.status.success() {
        print_output(&output);
        bail!("{label} failed with {}", output.status);
    }
    Ok(output)
}

pub(crate) fn run_command_output_allow_failure(
    command: &mut Command,
    label: &str,
) -> Result<Output> {
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
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{electron_builder_identity, parse_keychain_list, write_base64_secret};

    #[test]
    fn electron_builder_identity_drops_the_certificate_type_prefix() {
        // electron-builder refuses a name with the prefix; codesign accepts both.
        for (identity, expected) in [
            (
                "Developer ID Application: With Coral Ltd (TEAM123456)",
                "With Coral Ltd (TEAM123456)",
            ),
            ("With Coral Ltd (TEAM123456)", "With Coral Ltd (TEAM123456)"),
            (
                "0123456789ABCDEF0123456789ABCDEF01234567",
                "0123456789ABCDEF0123456789ABCDEF01234567",
            ),
        ] {
            assert_eq!(electron_builder_identity(identity), expected);
        }
    }

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
