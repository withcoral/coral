# Documentation guidance

- Public pages are MDX with YAML frontmatter. Navigation and Mintlify settings
  live in `docs.json`.
- Document public behavior and the minimum operational knowledge that readers
  need. Prefer the best existing page over a new page, and do not publish
  internal agent process.
- Use second person, root-relative internal links, language tags on code blocks,
  and alt text on images. Test code examples.

## Publishing

- The live site deploys from the long-lived `docs` branch, not from `main`.
- `main` still owns docs freshness. A merge to `main` does not publish the site.
- The release workflow advances `docs` only after it publishes the matching
  release. There is no separate immediate path for a docs-only release.
