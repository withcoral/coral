# Google Contacts Connector

This source queries the [Google People API](https://developers.google.com/people)
to expose contacts and contact groups as queryable SQL tables.

## Auth

Use Coral's interactive OAuth flow to connect Google Contacts:

```bash
coral source add --interactive --file sources/community/google_contacts/manifest.yaml
```

Choose **Connect Google Contacts** when Coral asks for the
`GOOGLE_CONTACTS_ACCESS_TOKEN` credential. Provide a Google OAuth Desktop app
client ID and client secret from a Google Cloud project with the **Google People API** enabled.

The OAuth flow requests the Contacts read-only scope:

```text
https://www.googleapis.com/auth/contacts.readonly
```

To add the source with an existing access token instead:

```bash
export GOOGLE_CONTACTS_ACCESS_TOKEN="<access-token>"
coral source add --file sources/community/google_contacts/manifest.yaml
```

Verify the connection and declared smoke queries:

```bash
coral source test google_contacts
```

## Start querying

Retrieve all contacts with primary display name and email address:

```sql
SELECT id, display_name, email, phone_number, organization
FROM google_contacts.contacts
LIMIT 50;
```

Find contacts belonging to a specific company/organization:

```sql
SELECT display_name, job_title, email
FROM google_contacts.contacts
WHERE organization = 'Google'
LIMIT 10;
```

List all contact groups (system groups and user-defined groups) and their member counts:

```sql
SELECT name, group_type, member_count, update_time
FROM google_contacts.contact_groups
WHERE deleted = false
ORDER BY member_count DESC;
```

Access the full lists of emails or phone numbers as arrays:

```sql
SELECT display_name, raw_email_addresses, raw_phone_numbers
FROM google_contacts.contacts
WHERE raw_email_addresses IS NOT NULL;
```

## Tables

### contacts

Contacts (connections) of the authenticated user. Maps to `GET /v1/people/me/connections`. 

Optional filters:
* `sort_order`: The sort order of connections. Supported values: `LAST_MODIFIED_ASCENDING`, `FIRST_NAME_ASCENDING`, `LAST_NAME_ASCENDING`.

Paginates using `pageToken` and `pageSize` up to 1000 items per page.

### contact_groups

Contact groups owned by the authenticated user. Maps to `GET /v1/contactGroups`.

Paginates using `pageToken` and `pageSize` up to 1000 items per page.

## Notes

- This source is read-only. It does not create, update, or delete contacts or groups.
- Google Contacts access tokens expire. Coral stores OAuth refresh metadata when Google returns it, but automatic token refresh is not implemented yet.
- The contacts list requires `personFields` which are hardcoded in the manifest to retrieve names, email addresses, phone numbers, organizations, biographies, and metadata.
- For contacts with multiple emails or phone numbers, `email` and `phone_number` select the first entry. All items are accessible via `emails`/`phone_numbers` (comma-separated strings) or `raw_email_addresses`/`raw_phone_numbers` (JSON arrays).
