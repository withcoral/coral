## Summary

Add a StatusGator community source for Coral so users can query monitored
services and recent incidents through StatusGator's API.

## What It Enables

- List monitored services and their current status.
- Query recent incidents and status updates.
- Expose raw JSON for downstream parsing and LLM workflows.
- Optional `STATUSGATOR_API_KEY` support for authenticated requests.

## Usage Examples

- List monitored services:

  SELECT id, name, status FROM statusgator.services LIMIT 20

- Show recent incidents:

  SELECT id, service_id, title, status FROM statusgator.incidents LIMIT 50

## Validation

- YAML parse:

  ruby -ryaml -e "YAML.load_file('sources/community/statusgator/manifest.yaml'); puts 'YAML OK'"

## Files

- `sources/community/statusgator/manifest.yaml`
- `sources/community/statusgator/README.md`
