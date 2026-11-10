# ee/ guidance

The root `AGENTS.md` applies here too. These rules add to it.

- Start every source file with
  `SPDX-License-Identifier: LicenseRef-Coral-Enterprise` in its first three
  lines. Set `license = "LicenseRef-Coral-Enterprise"` and `publish = false`
  on each crate. `scripts/check-ee-boundary.sh` checks the headers.
- When an Enterprise Feature needs something new from the rest of Coral, add a
  general extension point outside `ee/` first, then implement it here. Reach
  it through `coral_app::Extensions`.
- An Enterprise Feature checks `EditionCheck::is_enterprise` each time it is
  used, not once at startup: a licence can change while Coral runs. Give each
  one a test that it refuses to work while the Edition is Community.
- Never add a migration here. Put the general schema in
  `crates/coral-db/migrations`.
