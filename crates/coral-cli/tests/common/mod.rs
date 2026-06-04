#![allow(
    dead_code,
    reason = "Integration test targets share this module, but each target only uses a subset."
)]

pub(crate) fn stdout(assert: &assert_cmd::assert::Assert) -> String {
    String::from_utf8_lossy(&assert.get_output().stdout).into_owned()
}

pub(crate) fn stderr(assert: &assert_cmd::assert::Assert) -> String {
    String::from_utf8_lossy(&assert.get_output().stderr).into_owned()
}

pub(crate) fn assert_contains(output: &str, expected: &str) {
    assert!(
        output.contains(expected),
        "expected output to contain {expected:?}: {output}"
    );
}

pub(crate) fn assert_contains_all(output: &str, expected: &[&str]) {
    for item in expected {
        assert_contains(output, item);
    }
}

pub(crate) fn assert_not_contains(output: &str, unexpected: &str) {
    assert!(
        !output.contains(unexpected),
        "expected output not to contain {unexpected:?}: {output}"
    );
}
