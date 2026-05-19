# Mintlify documentation

## Working relationship
- You can push back on ideas-this can lead to better documentation. Cite sources and explain your reasoning when you do so
- ALWAYS ask for clarification rather than making assumptions
- NEVER lie, guess, or make up information

## Project context
- Format: MDX files with YAML frontmatter
- Config: docs.json for navigation, theme, settings
- Components: Mintlify components

## Content strategy
- Document just enough for user success - not too much, not too little
- Prioritize accuracy and usability of information
- Make content evergreen when possible
- Search for existing information before adding new content. Avoid duplication unless it is done for a strategic reason
- Check existing patterns for consistency
- Start by making the smallest reasonable changes
- Keep `docs/` aligned with CLI and MCP user-facing surfaces; if commands,
  flags, output contracts, tools, resources, prompts, or workflows change
  elsewhere in the repo, update the relevant docs in the same change

## Maintaining these instructions
- Keep docs-specific agent rules here; keep repo-wide agent and contributor
  rules in the root `AGENTS.md`
- If human feedback exposes a repeated docs workflow failure, update this file,
  the nearest docs source, or docs tooling in the same change
- Do not add public MDX pages for internal agent process unless the process is
  itself useful to Coral docs readers

## Frontmatter requirements for pages
- title: Clear, descriptive page title
- description: Concise summary for SEO/navigation

## Writing standards
- Second-person voice ("you")
- Prerequisites at start of procedural content
- Test all code examples before publishing
- Match style and formatting of existing pages
- Include both basic and advanced use cases
- Language tags on all code blocks
- Alt text on all images
- Root-relative paths for internal links

## Do not
- Skip frontmatter on any MDX file
- Use absolute URLs for internal links
- Include untested code examples
- Make assumptions - always ask for clarification
