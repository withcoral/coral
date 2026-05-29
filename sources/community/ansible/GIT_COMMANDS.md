# Git Commands for Coral Contribution

Run these commands inside your local `withcoral/coral` clone.

This guide assumes the source lives at:

```text
sources/community/ansible/
```

and the working branch is:

```text
ansible-specs
```

## 1. Inspect current state

```bash
git status
git remote -v
git branch --show-current
git branch --list
git branch -r --list
```

Check recent commits:

```bash
git log --oneline -5
```

## 2. Find old Ansible branches

```bash
git branch --list '*ansible*'
git branch -r --list '*ansible*'
```

## 3. Delete old local branches only if safe

Only do this if you are sure the branches do not contain work you need.

```bash
git switch main
git pull --ff-only

git branch -D ansible-specs 2>/dev/null || true
git branch -D feat/community-ansible 2>/dev/null || true
git branch -D feat/community-ansible-facts-source 2>/dev/null || true
```

PowerShell version:

```powershell
git switch main
git pull --ff-only

git branch -D ansible-specs
git branch -D feat/community-ansible
git branch -D feat/community-ansible-facts-source
```

If a branch does not exist, Git will print an error. That is okay.

## 4. Create a clean branch

```bash
git switch main
git pull --ff-only
git switch -c ansible-specs
```

## 5. Copy the source folder

If the `ansible/` folder is outside the Coral repo:

```bash
mkdir -p sources/community/ansible
cp -R /path/to/ansible/* sources/community/ansible/
```

PowerShell version:

```powershell
New-Item -ItemType Directory -Force sources\community\ansible
Copy-Item C:\path\to\ansible\* sources\community\ansible\ -Recurse -Force
```

## 6. Check line endings

On Windows, make sure source files are LF, not CRLF:

```bash
git ls-files --eol sources/community/ansible/manifest.yaml
```

Expected:

```text
i/lf    w/lf
```

If the whole repo has CRLF problems, configure Git and reclone:

```bash
git config --global core.autocrlf false
git config --global core.eol lf
```

## 7. Validate fixtures

From the Coral repo root:

```bash
python sources/community/ansible/tests/validate-fixtures.py sources/community/ansible/fixtures
```

PowerShell fallback:

```powershell
py sources\community\ansible\tests\validate-fixtures.py sources\community\ansible\fixtures
```

## 8. Prepare fixture data for local Coral test

Linux/macOS:

```bash
mkdir -p ~/.coral/ansible-facts
cp sources/community/ansible/fixtures/*.jsonl ~/.coral/ansible-facts/
```

Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force C:\tmp\coral-ansible-facts
Copy-Item sources\community\ansible\fixtures\*.jsonl C:\tmp\coral-ansible-facts\ -Force
```

## 9. Windows-only local manifest

The committed manifest uses:

```yaml
location: file://~/.coral/ansible-facts/
```

On Windows, the same home-directory location is used by the Coral CLI:

```powershell
Copy-Item `
  sources\community\ansible\manifest.yaml `
  sources\community\ansible\manifest.windows.local.yaml `
  -Force

$manifest = Join-Path (Get-Location) "sources\community\ansible\manifest.windows.local.yaml"
$text = [System.IO.File]::ReadAllText($manifest)
$text = $text.Replace(
  "file://~/.coral/ansible-facts/",
  "file://~/.coral/ansible-facts/"
)
$text = $text -replace "`r`n", "`n"
[System.IO.File]::WriteAllText(
  $manifest,
  $text,
  [System.Text.UTF8Encoding]::new($false)
)

Add-Content .git\info\exclude "sources/community/ansible/manifest.windows.local.yaml"
```

Verify it is ignored:

```powershell
git status --ignored
git check-ignore -v sources/community/ansible/manifest.windows.local.yaml
```

`git ls-files` should print nothing.

## 10. Run Coral source lint

Using installed Coral:

```bash
coral source lint sources/community/ansible/manifest.yaml
```

Using repo-built Coral CLI:

```bash
cargo run --locked -p coral-cli -- source lint sources/community/ansible/manifest.yaml
```

Expected:

```text
Manifest is valid
```

For Windows local test:

```powershell
cargo run --locked -p coral-cli -- source lint sources/community/ansible/manifest.windows.local.yaml
```

## 11. Add and test the source

Linux/macOS:

```bash
coral source add --file sources/community/ansible/manifest.yaml
coral source test ansible
```

Windows local test:

```powershell
cargo run --locked -p coral-cli -- source add --file sources/community/ansible/manifest.windows.local.yaml
cargo run --locked -p coral-cli -- source test ansible
```

Expected:

```text
ansible connected successfully
7 tables discovered
5 declared query tests passed
0 failed
```

## 12. Query the source

```bash
coral sql "
  SELECT schema_name, table_name
  FROM coral.tables
  WHERE schema_name = 'ansible'
  ORDER BY table_name
"
```

```bash
coral sql "
  SELECT hostname, distribution, service_mgr, pkg_mgr
  FROM ansible.hosts
  ORDER BY hostname
"
```

Repo-built CLI version:

```bash
cargo run --locked -p coral-cli -- sql "SELECT hostname, distribution, service_mgr, pkg_mgr FROM ansible.hosts ORDER BY hostname"
```

## 13. Run repository source linter

```bash
make lint-sources
```

Equivalent direct command:

```bash
ryl sources
```

If formatting changes are needed:

```bash
ryl --fix sources
```

Then inspect changes:

```bash
git diff
```

## 14. Rebase onto latest main

```bash
git fetch origin
git rebase origin/main
```

If conflicts appear, stop and resolve them carefully. After resolving:

```bash
git status
git add <resolved-files>
git rebase --continue
```

Then rerun:

```bash
ryl sources
make lint-sources
coral source lint sources/community/ansible/manifest.yaml
```

## 15. Check what will be committed

```bash
git status
git diff --stat
git diff -- sources/community/ansible/manifest.yaml
git diff -- sources/community/ansible/README.md
```

Check ignored files:

```bash
git status --ignored
```

Make sure these are not staged:

```text
manifest.windows.local.yaml
target/
ui/dist/
ui/node_modules/
ui/src/generated/
raw-facts/
normalized-facts/
```

## 16. Commit

```bash
git add sources/community/ansible
git commit -m "feat(sources/community/ansible): add Ansible facts source"
```

If you are polishing an existing local commit before PR:

```bash
git add sources/community/ansible
git commit --amend --no-edit
```

## 17. Push

First push:

```bash
git push -u origin ansible-specs
```

If you rebased after pushing:

```bash
git push --force-with-lease
```

Do not use plain `--force`.

## 18. Create PR

```bash
gh pr create \
  --title "feat(sources/community/ansible): add Ansible facts source" \
  --body "Adds a file-backed community source for sanitized Ansible fact exports. Includes manifest, README, fixtures, example gather/normalize scripts, security notes, design notes, and example SQL queries."
```

## 19. Final PR checklist

Before opening or updating the PR, run:

```bash
git status
python sources/community/ansible/tests/validate-fixtures.py sources/community/ansible/fixtures
make lint-sources
coral source lint sources/community/ansible/manifest.yaml
coral source test ansible
```

Also verify:

```text
No real secrets
No real private keys
No real Vault values
No real company inventory
No generated real facts
No Windows-only manifest committed
No build artifacts committed
Branch rebased on latest origin/main
PR title uses Conventional Commit format
```
