StatusGator community source for Coral
=====================================

Setup
-----

- (Optional) Provide a `STATUSGATOR_API_KEY` if you have one. Example:

  export STATUSGATOR_API_KEY="secret_..."

- If using a private StatusGator account or API, ensure the key has read
  permissions for services and incidents.

Quick example queries
---------------------

- List monitored services:

  SELECT id, name, status FROM statusgator.services LIMIT 20

- Show recent incidents:

  SELECT id, service_id, title, status FROM statusgator.incidents LIMIT 50

Notes
-----

- The manifest uses `base_url: https://statusgator.com/api` and maps
  common endpoints used by StatusGator-style status APIs. If your
  organization uses a different base path, update `base_url` or provide
  a `STATUSGATOR_API_KEY` as needed.
