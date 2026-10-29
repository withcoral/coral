//! The values a span or event field may carry.
//!
//! The export filters keep only the keys the field catalog lists, but under a
//! cataloged key a filter cannot tell an identifier from an error message. So
//! every field value is a literal, `Empty`, or the result of one of these
//! functions, and a source check fails on any other. Each takes only a type
//! that cannot hold caller text, an error's text or a credential:
//!
//! - an [`Identifier`], whose text Coral minted or resolved;
//! - a `&'static str`, text fixed when Coral was built, such as a code;
//! - a number or a flag.
//!
//! What stays trusted is each `impl Identifier`, which says why its type
//! qualifies, and that a `&'static str` is build-time text: the source check
//! fails on a call that leaks a runtime string to `'static`.

use std::fmt;
use std::sync::LazyLock;

/// A value whose text names something Coral minted, serves or runs: an ID
/// in a format Coral issues (a UUID, or words from Coral's word list), or a
/// name Coral resolved against something it holds, such as the Call Path of
/// a Tool it found.
///
/// Implement it only when every value of the type is one of these. Looking
/// like an ID is not enough: a type built from caller text before Coral looks
/// it up does not qualify, and nor does a Principal's ID, which leaves only
/// through an authenticated Principal. Each implementation says why its type
/// qualifies.
pub trait Identifier: fmt::Display {}

impl<T: Identifier + ?Sized> Identifier for &T {}

/// Text fixed when Coral was built, such as a name Coral serves.
impl Identifier for &'static str {}

/// A UUID holds no text.
impl Identifier for uuid::Uuid {}

/// The field value of the identifier `value`.
pub fn id<T: Identifier + ?Sized>(value: &T) -> tracing::field::DisplayValue<&T> {
    tracing::field::display(value)
}

/// The field value of a bounded kind or code, such as a state's storage
/// name or an error's `code()`. Only text fixed at build time is
/// `'static`.
#[must_use]
pub const fn kind(value: &'static str) -> &'static str {
    value
}

/// The field value of a count, a size, a duration or a status number.
pub fn number<T: Number>(value: T) -> T {
    value
}

/// The field value of a flag.
#[must_use]
pub const fn flag(value: bool) -> bool {
    value
}

/// The name of a span for `operation` on `target`: `{operation} {target}`,
/// or `operation` alone without a target.
#[must_use]
pub fn span_name<T: Identifier + ?Sized>(operation: &'static str, target: Option<&T>) -> String {
    target.map_or_else(
        || operation.to_owned(),
        |target| format!("{operation} {target}"),
    )
}

/// The name of an HTTP span: `{method} {route}`, the method alone without a
/// route, and `HTTP` for a method outside the standard set.
#[must_use]
pub fn http_span_name<T: Identifier + ?Sized>(method: &http::Method, route: Option<&T>) -> String {
    match http_method(method) {
        "_OTHER" => span_name("HTTP", route),
        method => span_name(method, route),
    }
}

/// The request method as a span field: a standard method's name, else
/// `_OTHER`, so a caller cannot choose the text.
#[must_use]
pub fn http_method(method: &http::Method) -> &'static str {
    match method.as_str() {
        "GET" => "GET",
        "HEAD" => "HEAD",
        "POST" => "POST",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        "CONNECT" => "CONNECT",
        "OPTIONS" => "OPTIONS",
        "TRACE" => "TRACE",
        "PATCH" => "PATCH",
        _ => "_OTHER",
    }
}

/// The text of every HTTP status code, from 100 to 999.
static HTTP_STATUS_CODES: LazyLock<Vec<String>> =
    LazyLock::new(|| (100..=999).map(|code| code.to_string()).collect());

/// An HTTP status as an `error.type`: its code, as the conventions name it.
#[must_use]
pub fn http_status_code(status: u16) -> &'static str {
    status
        .checked_sub(100)
        .and_then(|index| HTTP_STATUS_CODES.get(usize::from(index)))
        .map_or("_OTHER", String::as_str)
}

/// The text of every JSON-RPC error code the protocol reserves, from -32768
/// to -32000.
static JSON_RPC_CODES: LazyLock<Vec<String>> = LazyLock::new(|| {
    (-32768..=-32000)
        .map(|code: i64| code.to_string())
        .collect()
});

/// A JSON-RPC error code as a span field, as the conventions name it, and
/// `_OTHER` for a code outside the reserved range.
#[must_use]
pub fn json_rpc_code(code: i64) -> &'static str {
    code.checked_add(32768)
        .and_then(|index| usize::try_from(index).ok())
        .and_then(|index| JSON_RPC_CODES.get(index))
        .map_or("_OTHER", String::as_str)
}

/// A number a field may carry.
pub trait Number: tracing::Value + sealed::Sealed {}

mod sealed {
    pub trait Sealed {}
}

macro_rules! numbers {
    ($($number:ty),*) => {
        $(
            impl sealed::Sealed for $number {}
            impl Number for $number {}
        )*
    };
}

numbers!(u8, u16, u32, u64, u128, usize, i32, i64);
