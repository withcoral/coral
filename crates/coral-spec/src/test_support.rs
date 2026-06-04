use std::fmt::Display;

use serde_json::Value;

pub(crate) fn assert_error_contains(error: &impl Display, expected: &str) {
    let message = error.to_string();
    assert!(
        message.contains(expected),
        "expected {message:?} to contain {expected:?}"
    );
}

pub(crate) fn insert_field(object: &mut Value, key: &str, value: Value) {
    object
        .as_object_mut()
        .expect("test fixture should be an object")
        .insert(key.to_string(), value);
}
