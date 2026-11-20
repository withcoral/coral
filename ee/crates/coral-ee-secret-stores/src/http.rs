// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! What every kind here needs to read from a store over HTTP.

use std::collections::BTreeMap;

use coral_outbound::OutboundUrlPolicyError;
use coral_secret_store::{Error, InvalidSetting};
use serde::Deserialize;
use serde_json::Value;
use url::Url;
use zeroize::{Zeroize as _, Zeroizing};

/// The largest answer Coral reads from a store.
pub(crate) const BODY_LIMIT: usize = 1 << 20;

/// A store's origin. This checks its shape only: the destination policy decides
/// whether Coral may reach it, and over which scheme, for every request.
pub struct StoreAddress(pub(crate) Url);

impl StoreAddress {
    /// # Errors
    /// Returns [`InvalidSetting`] for anything but an http or https origin.
    pub fn parse(value: &str) -> Result<Self, InvalidSetting> {
        Url::parse(value)
            .ok()
            .filter(|url| {
                matches!(url.scheme(), "http" | "https")
                    && url.host().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none()
            })
            .map(Self)
            .ok_or(InvalidSetting::new(
                "must be an http or https origin, without a path, query or credentials",
            ))
    }
}

/// The answer's body, wiped when dropped, up to [`BODY_LIMIT`].
pub(crate) async fn read_body(response: reqwest::Response) -> Result<Zeroizing<Vec<u8>>, Error> {
    coral_outbound::read_bounded_body(response, BODY_LIMIT)
        .await
        .map(Zeroizing::new)
        .map_err(|error| match error {
            OutboundUrlPolicyError::BodyTooLarge { .. } => Error::MalformedResponse,
            _ => Error::Unreachable,
        })
}

/// An entry's fields, wiped when dropped: the answer holds every value in the
/// entry, including those nobody asked for.
#[derive(Deserialize)]
pub(crate) struct Fields(pub(crate) BTreeMap<String, Value>);

impl Drop for Fields {
    fn drop(&mut self) {
        fn wipe(value: &mut Value) {
            match value {
                Value::String(text) => text.zeroize(),
                Value::Array(items) => items.iter_mut().for_each(wipe),
                Value::Object(fields) => fields.values_mut().for_each(wipe),
                Value::Null | Value::Bool(_) | Value::Number(_) => {}
            }
        }
        self.0.values_mut().for_each(wipe);
    }
}
