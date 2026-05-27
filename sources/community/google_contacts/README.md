# Google Contacts Connector

This source queries the [Google People API](https://developers.google.com/people)
to expose contacts and contact groups as queryable SQL tables.

## Auth & Setup Guide

To connect Google Contacts, you must first configure a Google Cloud project and an OAuth consent screen to obtain your Client ID and Client Secret:

### 1. Enable People API
1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new project or select an existing one.
3. Search for the **Google People API** in the API Library and click **Enable**.

### 2. Configure OAuth Consent Screen
1. In the sidebar, select **APIs & Services** > **OAuth consent screen**.
2. Select **External** user type and click **Create**.
3. Complete the app information (App Name, Support Email, Developer Email) and click **Save and Continue**.
4. In the **Scopes** step, click **Add or Remove Scopes**, manually paste/check the following scope:
   * `https://www.googleapis.com/auth/contacts.readonly`
   * Click **Update**, then **Save and Continue**.
5. In the **Test Users** step, click **Add Users** and add the Google Account email you want to query. **(This step is critical; without it, authorization will fail with a "restricted developer" warning)**. Save and continue.

### 3. Create Credentials
1. In the sidebar, select **APIs & Services** > **Credentials**.
2. Click **Create Credentials** at the top and select **OAuth client ID**.
3. Choose **Desktop app** as the Application type, give it a name (e.g. `Coral Contacts`), and click **Create**.
4. Copy the generated **Client ID** and **Client Secret**.

### 4. Add the Source in Coral
Run the interactive wizard:

```bash
coral source add --interactive --file sources/community/google_contacts/manifest.yaml
```

1. Choose **Connect Google Contacts**.
2. Paste the **OAuth Client ID** and **OAuth Client Secret** when prompted.
3. Complete the authentication flow in your browser.

To add the source with an existing access token directly:

```bash
export GOOGLE_CONTACTS_ACCESS_TOKEN="<access-token>"
coral source add --file sources/community/google_contacts/manifest.yaml
```

Verify the connection and declared smoke queries:

```bash
coral source test google_contacts
```

## Start querying

Retrieve all contacts with their first returned display name, email, phone number, and organization:

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
ORDER BY member_count DESC;
```

Query group memberships for each contact:

```sql
SELECT display_name, contact_groups, raw_memberships
FROM google_contacts.contacts
WHERE contact_groups IS NOT NULL;
```

Access the full list of emails or phone numbers as JSON arrays:

```sql
SELECT display_name, raw_email_addresses, raw_phone_numbers
FROM google_contacts.contacts
WHERE raw_email_addresses IS NOT NULL;
```

## Tables

### contacts

Contacts (connections) of the authenticated user. Maps to `GET /v1/people/me/connections`. 

Optional filters:
* `sort_order`: The sort order of connections. Supported values: `LAST_MODIFIED_ASCENDING`, `LAST_MODIFIED_DESCENDING`, `FIRST_NAME_ASCENDING`, `LAST_NAME_ASCENDING`.

Paginates using `pageToken` and `pageSize` up to 1000 items per page.

### contact_groups

Contact groups owned by the authenticated user. Maps to `GET /v1/contactGroups`.

Paginates using `pageToken` and `pageSize` up to 1000 items per page.

## Notes

- This source is read-only. It does not create, update, or delete contacts or groups.
- Google Contacts access tokens expire. Coral stores OAuth refresh metadata when Google returns it, but automatic token refresh is not implemented yet.
- The contacts list requires `personFields` which are hardcoded in the manifest to retrieve names, email addresses, phone numbers, organizations, biographies, metadata, and memberships.
- **Array Order & Primary Items**: Google Person resource multi-item arrays (like names, email addresses, phone numbers, and organizations) do not guarantee order. The primary item is identified by the `metadata.primary: true` flag. The columns `display_name`, `family_name`, `given_name`, `email`, `phone_number`, `organization`, and `job_title` extract the first returned array element for convenience. For correctness-sensitive queries, use `raw_email_addresses`, `raw_phone_numbers`, or the full raw record and filter by the primary metadata flag.
- **Group Memberships**: The contact's group memberships are exposed through the `contact_groups` column (comma-separated resource name string) and the `raw_memberships` JSON column.

