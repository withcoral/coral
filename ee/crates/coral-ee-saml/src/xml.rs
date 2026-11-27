// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! Text that Coral writes into XML.

use std::borrow::Cow;

/// `value` escaped for an attribute value in double quotes or for element
/// text.
pub(crate) fn escape(value: &str) -> Cow<'_, str> {
    if !value.contains(['&', '<', '>', '"']) {
        return Cow::Borrowed(value);
    }
    let mut escaped = String::with_capacity(value.len() + 8);
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            other => escaped.push(other),
        }
    }
    Cow::Owned(escaped)
}
