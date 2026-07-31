Source: https://docs.slack.dev/reference/scopes/admin

# admin scope

Administer a workspace

## Facts

**Supported token types**

[`User`](/authentication/tokens#user)

**Compatible API methods**

[`admin.audit.anomaly.allow.getItem`](/reference/methods/admin.audit.anomaly.allow.getitem)

[`admin.audit.anomaly.allow.updateItem`](/reference/methods/admin.audit.anomaly.allow.updateitem)

[`team.accessLogs`](/reference/methods/team.accesslogs)

[`team.billableInfo`](/reference/methods/team.billableinfo)

[`team.integrationLogs`](/reference/methods/team.integrationlogs)

## Usage info {#usage-info}

This scope is quite special. It grants miscellaneous admin capabilities to the app that requests it, provided the installer is an admin allowed to grant those abilities.

It allows access to [`team.accessLogs`](/reference/methods/team.accessLogs) and the [SCIM API](/admins/scim-api).

This is not an all-inclusive list of admin token capabilities.
