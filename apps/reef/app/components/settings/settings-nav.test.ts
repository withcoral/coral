import { expect, test } from 'vitest'
import { currentSettingsItem, settingsNav } from './settings-nav'

const sections = (reader: Parameters<typeof settingsNav>[0]) =>
  settingsNav(reader).map((item) => [item.label, item.to])

// Catches: a member offered a section Coral refuses them, a local deployment,
// where nobody else can sign in, offered people to manage, or a deployment that
// turned secret stores off, such as Desktop, offered a page that does not exist.
test.each([
  [
    'an organization owner',
    { organizationOwner: true, people: true, secretStoresEnabled: true },
    [
      ['Members', '/organization/members'],
      ['Workspaces', '/organization/workspaces'],
      ['Secret stores', '/organization/secret-stores'],
      ['Sub-agent', '/organization/sub-agent'],
      ['Usage data', '/usage-data'],
    ],
  ],
  [
    'an organization member',
    { organizationOwner: false, people: true, secretStoresEnabled: true },
    [],
  ],
  [
    'a local deployment',
    { organizationOwner: true, people: false, secretStoresEnabled: true },
    [
      ['Workspaces', '/organization/workspaces'],
      ['Secret stores', '/organization/secret-stores'],
      ['Sub-agent', '/organization/sub-agent'],
      ['Usage data', '/usage-data'],
    ],
  ],
  [
    'a deployment that turned secret stores off',
    { organizationOwner: true, people: false, secretStoresEnabled: false },
    [
      ['Workspaces', '/organization/workspaces'],
      ['Sub-agent', '/organization/sub-agent'],
      ['Usage data', '/usage-data'],
    ],
  ],
])('%s sees only the organization sections they can open', (_, who, expected) => {
  expect(sections(who)).toEqual(expected)
})

// Catches: a section losing its mark, or a page of work marking one.
test.each([
  ['/organization/members', 'members'],
  ['/organization/workspaces', 'workspaces'],
  ['/organization/sub-agent', 'sub-agent'],
  ['/usage-data', 'usage-data'],
  ['/workspaces/ops/members', undefined],
])('at %s the current section is %s', (pathname, expected) => {
  const nav = settingsNav({ organizationOwner: true, people: true, secretStoresEnabled: true })
  expect(currentSettingsItem(nav, pathname)?.id).toBe(expected)
})
