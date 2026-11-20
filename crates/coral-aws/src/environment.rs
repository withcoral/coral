//! The environment `aws-config` reads, as bootstrap saw it.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt;
use std::sync::Arc;

/// A snapshot of the variables AWS credential lookup reads: every `AWS_*`
/// variable, those that locate the home directory, where the AWS config files
/// live, and the HTTPS proxy settings. Bootstrap takes it once; nothing here
/// reads the process environment.
///
/// It holds credentials, so its `Debug` shows only the variables' names.
#[derive(Clone, Default)]
pub struct Environment(Arc<HashMap<String, String>>);

/// The variables outside `AWS_*` that the snapshot keeps: those `aws-config`
/// reads to find `~/.aws`, and the proxy settings.
const KEPT: [&str; 10] = [
    "HOME",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
];

impl Environment {
    /// Keeps the variables `aws-config` reads from `vars`, and drops the rest.
    /// A variable whose name or value is not Unicode is dropped.
    #[must_use]
    pub fn snapshot(vars: impl IntoIterator<Item = (OsString, OsString)>) -> Self {
        Self(Arc::new(
            vars.into_iter()
                .filter_map(|(name, value)| {
                    Some((name.into_string().ok()?, value.into_string().ok()?))
                })
                .filter(|(name, _)| name.starts_with("AWS_") || KEPT.contains(&name.as_str()))
                .collect(),
        ))
    }

    /// The HTTPS proxy, `HTTPS_PROXY` or else `ALL_PROXY`, and the hosts that
    /// bypass it, `NO_PROXY`. Each name may also be lowercase.
    pub(crate) fn https_proxy(&self) -> Option<(&str, Option<&str>)> {
        let var = |name: &str| {
            self.0
                .get(name)
                .or_else(|| self.0.get(&name.to_ascii_lowercase()))
                .map(String::as_str)
                .filter(|value| !value.is_empty())
        };
        let proxy = var("HTTPS_PROXY").or_else(|| var("ALL_PROXY"))?;
        Some((proxy, var("NO_PROXY")))
    }

    pub(crate) fn to_aws(&self) -> aws_types::os_shim_internal::Env {
        aws_types::os_shim_internal::Env::from((*self.0).clone())
    }
}

impl fmt::Debug for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<&str> = self.0.keys().map(String::as_str).collect();
        names.sort_unstable();
        f.debug_tuple("Environment").field(&names).finish()
    }
}
