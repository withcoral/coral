//! The settings an identity is built from, parsed by its caller.
//!
//! An operator may paste a credential into the wrong setting, so no error
//! repeats a value: each names its setting and a reason.

/// Why a setting is unusable. Boot stops on any of these.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SettingsError {
    /// `account_id` is not a 12-digit AWS account ID.
    #[error("account_id must be a 12-digit AWS account ID")]
    AccountId,
    /// `region` is empty or not a region name.
    #[error("region must be an AWS region name, such as eu-west-2")]
    Region,
    /// `profile` is given but blank.
    #[error("profile must not be empty")]
    Profile,
    /// `role_arn` is not the ARN of an IAM role.
    #[error("role_arn must be the ARN of an IAM role")]
    RoleArn,
    /// `role_arn` names a role in another account than `account_id`.
    #[error("role_arn must name a role in the account account_id names")]
    RoleInAnotherAccount,
    /// `coral-<name>` is not a valid role session name.
    #[error(
        "the name must be at most 58 characters of letters, digits and +=,.@_- to name a role session"
    )]
    SessionName,
    /// The `HTTPS_PROXY` or `ALL_PROXY` environment variable is not a proxy URL.
    #[error("the HTTPS proxy must be an http or https URL")]
    HttpsProxy,
}

impl SettingsError {
    /// The setting at fault, as an operator writes it.
    #[must_use]
    pub const fn field(self) -> &'static str {
        match self {
            Self::AccountId => "account_id",
            Self::Region => "region",
            Self::Profile => "profile",
            Self::RoleArn | Self::RoleInAnotherAccount => "role_arn",
            Self::SessionName => "name",
            Self::HttpsProxy => "HTTPS_PROXY",
        }
    }
}

/// One identity's checked settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub(crate) aws_account_id: String,
    pub(crate) region: String,
    pub(crate) profile: Option<String>,
    pub(crate) role_arn: Option<String>,
}

impl Settings {
    /// Checks the settings.
    ///
    /// - `aws_account_id`: the account Coral expects its credentials to be
    ///   in, which the `account_id` setting names.
    /// - `region`: the region to sign for and to reach STS in.
    /// - `profile`: a profile in the AWS config file. Without one, the
    ///   default chain finds the credentials.
    /// - `role_arn`: a role to assume on top of those credentials. It must be
    ///   in `aws_account_id`.
    ///
    /// # Errors
    /// Returns the first unusable setting.
    pub fn new(
        aws_account_id: &str,
        region: &str,
        profile: Option<&str>,
        role_arn: Option<&str>,
    ) -> Result<Self, SettingsError> {
        if !is_account_id(aws_account_id) {
            return Err(SettingsError::AccountId);
        }
        if region.is_empty()
            || !region
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(SettingsError::Region);
        }
        if profile.is_some_and(|profile| profile.trim().is_empty()) {
            return Err(SettingsError::Profile);
        }
        if let Some(role_arn) = role_arn
            && role_account(role_arn).ok_or(SettingsError::RoleArn)? != aws_account_id
        {
            return Err(SettingsError::RoleInAnotherAccount);
        }
        Ok(Self {
            aws_account_id: aws_account_id.to_owned(),
            region: region.to_owned(),
            profile: profile.map(str::to_owned),
            role_arn: role_arn.map(str::to_owned),
        })
    }
}

fn is_account_id(value: &str) -> bool {
    value.len() == 12 && value.bytes().all(|b| b.is_ascii_digit())
}

/// The account of an IAM role ARN, `arn:<partition>:iam::<account>:role/<name>`.
fn role_account(arn: &str) -> Option<&str> {
    let mut parts = arn.splitn(6, ':');
    let (Some("arn"), Some(partition), Some("iam"), Some(""), Some(account), Some(resource)) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return None;
    };
    let name = resource.strip_prefix("role/")?;
    let valid = partition.starts_with("aws")
        && is_account_id(account)
        && !name.is_empty()
        && !name.ends_with('/');
    valid.then_some(account)
}

/// The role session name for the identity called `name`.
pub(crate) fn session_name(name: &str) -> Result<String, SettingsError> {
    let valid = !name.is_empty()
        && name.len() <= 58
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "+=,.@_-".contains(c));
    if valid {
        Ok(format!("coral-{name}"))
    } else {
        Err(SettingsError::SessionName)
    }
}
