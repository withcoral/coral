Please post this in the PR conversation to request review:

---
Hi @simonwhitaker and @withcoral/sources —

I added a StatusGator community source (`sources/community/statusgator`) that
exposes monitored services and recent incidents. The branch is
`feat/statusgator-source` on my fork and the PR is ready for review.

Highlights:
- `manifest.yaml` (dsl v3) for services and incidents
- `README.md` with setup and example queries

Validation:
- YAML parse: `ruby -ryaml -e "YAML.load_file('sources/community/statusgator/manifest.yaml'); puts 'YAML OK'"`

PR link (create or paste once opened):
https://github.com/garimapahwa/coral/pull/new/feat/statusgator-source

Thanks in advance for any review and feedback!

---
