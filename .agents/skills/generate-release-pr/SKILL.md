---
name: generate-release-pr
description: Generates a release PR. Use when the user wants to generate a release PR with the latest changes.
allowed-tools:
  - Bash(git tag *)
  - Bash(git log *)
  - Bash(git checkout -b *)
  - Bash(git fetch *)
  - Bash(cargo generate-lockfile)
  - Read
---

# Generate a release PR

This skill will create a new branch, update the version number, and create a pull request with the changes.

The canonical source of the project's current version is the `workspace.package.version` field in the root Cargo.toml file.

## Preconditions

- Run `git fetch` to ensure you have the latest commits and tags from the remote repository.

## Determining the new version number

- If the user specifies a version number, do a quick sanity check (ensure it follows semantic versioning, and is greater than the current version) and use that as the new version number. Proceed to updating the version in Cargo.toml and creating the PR.
- The commit the current release was built from will be tagged with `vX.Y.Z` where X.Y.Z is the version number.
- The commits that will be included in this release can therefore be determined with `git log vX.Y.Z..origin/main`, where `vX.Y.Z` is the tag of the last release. This will show all commits since the last release.
- Use the commit messages in the commits since the last release to determine the type of release (major, minor, patch). Commit messages use the conventional commit format, with or without scope (so e.g. both `feat: add new feature` and `fix(cli): fix bug` are valid). Breaking changes are indicated by an exclamation mark immediately before the colon (e.g. `feat!: change API` or `fix(cli)!: change CLI behavior`).
- Show the user a summary of the commits since the last release, grouped by type (features, fixes, breaking changes). For example:

  ```
  Commits since last release:
  - Features:
    - feat: add new feature
  - Fixes:
    - fix(cli): fix bug
  - Breaking changes:
    - feat!: change API
  ```

- Tell the user the type of release that will be generated based on the commits (e.g. "Based on the commits since the last release, this will be a minor release."). Offer them the option to choose a different release type if they want (e.g. "If you want to generate a different type of release, please choose one of the following options: major, minor, patch").
- Based on the user's choice, determine the new version number using semantic versioning rules. For example, if the current version is 1.2.3 and the user chooses a minor release, the new version would be 1.3.0. Bear in mind that if the current major version is 0, the rules are different: a minor release would increment the patch version (e.g. from 0.2.3 to 0.2.4) and a major release would increment the minor version and reset the patch version (e.g. from 0.2.3 to 0.3.0).

## Updating the version number

- Update the version number in the root Cargo.toml file.
- Run `cargo generate-lockfile` to update the Cargo.lock file with the new version.

## Creating a new branch and pull request

- Create a new branch named `release/vX.Y.Z` where X.Y.Z is the new version number.
- Commit the changes to the Cargo.toml and Cargo.lock files with a message like "chore: release vX.Y.Z".
- Push the new branch to the remote repository.
- Create a pull request from the new branch
  - use "chore: release vX.Y.Z" as the PR title
  - in the PR body, include the following copy:

  Commits in this release: https://github.com/withcoral/coral/compare/v[previous release version]...main
