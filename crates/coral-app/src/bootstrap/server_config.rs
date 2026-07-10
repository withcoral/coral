//! Configuration owned by the long-running Coral server surface.

#![cfg_attr(
    not(test),
    expect(dead_code, reason = "consumed by the descendant coral serve command")
)]

use std::net::{Ipv4Addr, SocketAddr};

use serde::Deserialize;

use super::AppError;
use crate::state::AppStateLayout;

#[derive(Debug, Default, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    server: ServerSettings,
}

/// Settings loaded from the top-level `[server]` section of `config.toml`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(crate) struct ServerSettings {
    pub(crate) bind_addr: SocketAddr,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        }
    }
}

impl ServerSettings {
    /// Loads only the `[server]` section, leaving other config ownership intact.
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        if !layout.config_file().try_exists()? {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(layout.config_file())?;
        Ok(toml::from_str::<ConfigFile>(&raw)?.server)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use tempfile::TempDir;

    use super::ServerSettings;
    use crate::state::AppStateLayout;

    #[test]
    fn defaults_to_ephemeral_ipv4_loopback_without_server_section() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");

        let settings = ServerSettings::load(&layout).expect("server settings");

        assert_eq!(
            settings.bind_addr,
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
        );
    }

    #[test]
    fn loads_bind_address_without_claiming_other_sections() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server]\nbind_addr = '127.0.0.2:14555'\n\n[future]\nvalue = true\n",
        )
        .expect("config file");

        let settings = ServerSettings::load(&layout).expect("server settings");

        assert_eq!(
            settings.bind_addr.ip(),
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))
        );
        assert_eq!(settings.bind_addr.port(), 14555);
    }

    #[test]
    fn rejects_invalid_bind_address() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server]\nbind_addr = 'localhost:14555'\n",
        )
        .expect("config file");

        ServerSettings::load(&layout).expect_err("bind address must be an IP socket address");
    }
}
