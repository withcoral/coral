from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

SCRIPT_PATH = Path(__file__).with_name("prepare_release.py")
SPEC = importlib.util.spec_from_file_location("prepare_release", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
prepare_release = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = prepare_release
SPEC.loader.exec_module(prepare_release)


class VersionTests(unittest.TestCase):
    def test_stable_release_is_newer_than_matching_prerelease(self) -> None:
        stable = prepare_release.Version.parse("v1.0.0")
        prerelease = prepare_release.Version.parse("v1.0.0-rc1")
        assert stable is not None
        assert prerelease is not None
        self.assertGreater(stable.sort_key(), prerelease.sort_key())

    def test_numeric_prerelease_parts_sort_numerically(self) -> None:
        left = prepare_release.Version.parse("v1.0.0-rc1.10")
        right = prepare_release.Version.parse("v1.0.0-rc1.2")
        assert left is not None
        assert right is not None
        self.assertGreater(left.sort_key(), right.sort_key())

    def test_latest_version_tag_ignores_non_semver_tags(self) -> None:
        latest = prepare_release.latest_version_tag(["v1.0.0-rc1", "v1.0.0", "vnext", "v0.9.0"])
        self.assertEqual(latest, "v1.0.0")


if __name__ == "__main__":
    unittest.main()
