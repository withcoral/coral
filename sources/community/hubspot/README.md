# HubSpot (Community)

**Version:** 0.1.0
**Backend:** HTTP (HubSpot CRM API v3)
**Tables:** 6
**Base URL:** `https://api.hubapi.com`

Query HubSpot CRM contacts, companies, deals, tickets, and owners from HubSpot
(Cloud). Designed for customer identity, pipeline visibility, and cross-source
joins with bundled **Stripe**, **Intercom**, and **Linear** on `email` and
company `domain`.

## Install

Community sources are not bundled with the Coral binary. Add the manifest from
this directory:

```bash
coral source add --file sources/community/hubspot/manifest.yaml
```

Or copy `manifest.yaml` into your workspace and pass that path to
`coral source add --file`.

Reference the linked GitHub issue in your PR so maintainers can connect the
contribution to the prior discussion.

## Authentication and setup

Requires `HUBSPOT_ACCESS_TOKEN` (private app Bearer token).

1. In HubSpot, go to **Settings → Integrations → Private Apps**.
2. Create an app with **read** scopes for the CRM objects you query, for example:
   - `crm.objects.contacts.read`
   - `crm.objects.companies.read`
   - `crm.objects.deals.read`
   - `crm.objects.tickets.read`
   - CRM owners read access
3. Copy the access token.

```bash
export HUBSPOT_ACCESS_TOKEN=pat-na1-...
coral source add --file sources/community/hubspot/manifest.yaml
```

See [HubSpot private apps](https://developers.hubspot.com/docs/apps/legacy-apps/private-apps/overview).

### HubSpot MCP vs Coral

HubSpot provides a [remote MCP server](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/integrate-with-hubspot-mcp-server)
for HubSpot-only agent workflows. Use this Coral source when you need **SQL joins**
and aggregations across HubSpot and other Coral sources in one query.

## Table categories

### CRM objects (list)

| Table | Description |
| --- | --- |
| `contacts` | People; primary join key `email` |
| `companies` | Accounts; join key `domain` |
| `deals` | Pipeline opportunities (`dealstage`, `pipeline`, `amount`) |
| `tickets` | HubSpot service tickets (not Intercom inbox conversations) |
| `owners` | CRM owners for `hubspot_owner_id` on other tables |

### Search

| Table | Description | Required filters |
| --- | --- | --- |
| `search_contacts` | Exact email lookup via CRM Search API | `email` |

## Filters and pagination

- List tables (`contacts`, `companies`, `deals`, `tickets`, `owners`) use HubSpot
  cursor pagination (`after`). Always use `LIMIT` on large portals.
- `search_contacts` requires `email` and is subject to HubSpot Search API rate
  limits (~4 requests per second per token).
- v1 exposes a fixed property set per table; portal-specific custom properties
  are out of scope.

## Example relationships

```text
hubspot.contacts.email
  → stripe.customers.email
  → intercom.contacts.email
  → linear.users.email

hubspot.companies.domain
  → account matching / enrichment

hubspot.contacts.hubspot_owner_id
  → hubspot.owners.id
```

## Example queries

### Contacts and owners

```sql
SELECT c.email, c.firstname, c.lastname, o.email AS owner_email
FROM hubspot.contacts c
LEFT JOIN hubspot.owners o ON c.hubspot_owner_id = o.id
LIMIT 20;
```

### Lookup by email

```sql
SELECT id, contact_email, firstname, lastname, lifecyclestage
FROM hubspot.search_contacts
WHERE email = 'customer@example.com'
LIMIT 5;
```

### Paying customers with open deals (requires Stripe)

```sql
SELECT s.email, s.id AS stripe_customer_id, d.dealname, d.dealstage
FROM stripe.customers s
JOIN hubspot.contacts c ON LOWER(s.email) = LOWER(c.email)
JOIN hubspot.deals d ON d.hubspot_owner_id = c.hubspot_owner_id
WHERE d.dealstage NOT IN ('closedwon', 'closedlost')
LIMIT 20;
```

### Intercom contacts missing from HubSpot (requires Intercom)

```sql
SELECT i.email, i.name
FROM intercom.contacts i
LEFT JOIN hubspot.contacts h ON LOWER(i.email) = LOWER(h.email)
WHERE i.email IS NOT NULL AND h.id IS NULL
LIMIT 50;
```

### Pipeline summary

```sql
SELECT dealstage, COUNT(*) AS deal_count
FROM hubspot.deals
GROUP BY dealstage
ORDER BY deal_count DESC;
```

## Validation

```bash
# YAML style (requires: cargo install ryl --locked)
make lint-sources

# Manifest structure and smoke queries (requires Coral CLI)
coral source lint sources/community/hubspot/manifest.yaml
export HUBSPOT_ACCESS_TOKEN=pat-...
coral source add --file sources/community/hubspot/manifest.yaml
coral source test hubspot
```

## Limitations

- **Read-only** v1 (no creates/updates).
- **HubSpot Cloud only** (`api.hubapi.com`); no private-host override in v1.
- **No associations API** — contact↔company↔deal links are a follow-up.
- **Fixed properties** — custom portal fields are not discovered dynamically.
- **Tickets vs Intercom** — use `hubspot.tickets` for CRM tickets;
  `intercom.conversations` for support inbox history.
- **Rate limits** — respect [HubSpot API usage](https://developers.hubspot.com/docs/api/usage-details);
  prefer `search_contacts` over full `contacts` scans when you know the email.
- Community sources are maintained separately from bundled core sources.

## Contributing

Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md): discuss on the issue first,
sign the CLA if this is your first contribution, run `make lint-sources`, and
open a focused PR titled `feat(sources/community/hubspot): add hubspot community source`.
