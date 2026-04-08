from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_PATH = Path(__file__).with_name("prepare_release.py")
SPEC = importlib.util.spec_from_file_location("prepare_release", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
prepare_release = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = prepare_release
SPEC.loader.exec_module(prepare_release)


class PrepareReleaseIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.repo = Path(self.tempdir.name)
        self.original_cwd = Path.cwd()
        os.chdir(self.repo)
        self.addCleanup(os.chdir, self.original_cwd)

        self.git("init", "-b", "main")
        self.git("config", "user.name", "Test User")
        self.git("config", "user.email", "test@example.com")

    def git(self, *args: str) -> str:
        result = subprocess.run(
            ["git", *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    def write_workspace_version(self, version: str) -> None:
        cargo_toml = (
            "[workspace]\n"
            "members = []\n\n"
            "[workspace.package]\n"
            f'version = "{version}"\n'
        )
        (self.repo / "Cargo.toml").write_text(cargo_toml, encoding="utf-8")

    def commit(self, message: str) -> str:
        self.git("add", ".")
        self.git("commit", "-m", message)
        return self.git("rev-parse", "HEAD")

    def call_prepare_outputs(self, **env: str) -> dict[str, str]:
        with mock.patch.dict(os.environ, env, clear=True):
            with contextlib.redirect_stdout(io.StringIO()):
                return prepare_release.prepare_outputs()

    def test_push_with_unchanged_version_skips_release(self) -> None:
        self.write_workspace_version("0.1.0")
        self.commit("initial release version")

        (self.repo / "README.md").write_text("docs update\n", encoding="utf-8")
        head_sha = self.commit("docs only change")

        outputs = self.call_prepare_outputs(
            GITHUB_EVENT_NAME="push",
            GITHUB_REF="refs/heads/main",
            GITHUB_SHA=head_sha,
        )

        self.assertEqual(outputs, {"should_release": "false"})

    def test_manual_release_accepts_stable_after_prerelease_tag(self) -> None:
        self.write_workspace_version("1.0.0-rc1")
        self.commit("release candidate")
        self.git("tag", "v1.0.0-rc1")

        self.write_workspace_version("1.0.0")
        stable_sha = self.commit("stable release")

        outputs = self.call_prepare_outputs(
            GITHUB_EVENT_NAME="workflow_dispatch",
            GITHUB_REF="refs/heads/main",
            GITHUB_SHA=stable_sha,
            TARGET_REF=stable_sha,
        )

        self.assertEqual(outputs["should_release"], "true")
        self.assertEqual(outputs["build_ref"], stable_sha)
        self.assertEqual(outputs["tag_name"], "v1.0.0")
        self.assertEqual(outputs["release_name"], "v1.0.0")

    def test_manual_release_rejects_duplicate_tag(self) -> None:
        self.write_workspace_version("1.0.0")
        release_sha = self.commit("stable release")
        self.git("tag", "v1.0.0")

        with self.assertRaisesRegex(prepare_release.ReleaseError, r"Tag v1\.0\.0 already exists\."):
            self.call_prepare_outputs(
                GITHUB_EVENT_NAME="workflow_dispatch",
                GITHUB_REF="refs/heads/main",
                GITHUB_SHA=release_sha,
                TARGET_REF=release_sha,
            )

    def test_manual_release_rejects_target_ref_outside_main(self) -> None:
        self.write_workspace_version("0.1.0")
        main_sha = self.commit("main release")

        self.git("checkout", "-b", "feature/test-release")
        self.write_workspace_version("0.2.0")
        feature_sha = self.commit("feature only release")
        self.git("checkout", "main")

        with self.assertRaisesRegex(prepare_release.ReleaseError, r"target_ref must point to a commit on main\."):
            self.call_prepare_outputs(
                GITHUB_EVENT_NAME="workflow_dispatch",
                GITHUB_REF="refs/heads/main",
                GITHUB_SHA=main_sha,
                TARGET_REF=feature_sha,
            )


if __name__ == "__main__":
    unittest.main()
