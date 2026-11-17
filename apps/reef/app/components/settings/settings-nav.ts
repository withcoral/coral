import type { IconName } from '@/wax/components/icon'

/**
 * The organization's settings: what you administer for everyone, away from the
 * work in a workspace. This module lists the sections of the settings nav and
 * who sees each. `routes.ts` puts each section's page under the settings layout.
 */

export interface SettingsNavItem {
  id: string
  label: string
  to: string
  icon: IconName
}

/** Where each section of the organization's settings lives. */
export const settingsPath = {
  organizationMembers: '/organization/members',
  organizationWorkspaces: '/organization/workspaces',
  secretStores: '/organization/secret-stores',
  usageData: '/usage-data',
}

export interface SettingsReader {
  organizationOwner: boolean
  /** A local deployment has no other people to manage. */
  people: boolean
  /** False when the deployment turned secret stores off, as Desktop does. */
  secretStores: boolean
}

/**
 * The organization's sections for this reader: only what they can open, in the
 * order it shows. It is never empty, and Settings in the account menu opens the first.
 */
export function settingsNav({
  organizationOwner,
  people,
  secretStores,
}: SettingsReader): SettingsNavItem[] {
  const sections: (SettingsNavItem | false)[] = [
    organizationOwner &&
      people && {
        id: 'members',
        label: 'Members',
        to: settingsPath.organizationMembers,
        icon: 'Users',
      },
    organizationOwner && {
      id: 'workspaces',
      label: 'Workspaces',
      to: settingsPath.organizationWorkspaces,
      icon: 'Folders',
    },
    organizationOwner &&
      secretStores && {
        id: 'secret-stores',
        label: 'Secret stores',
        to: settingsPath.secretStores,
        icon: 'KeyRound',
      },
    // Every member can read what Coral sends; only organization owners change it.
    {
      id: 'usage-data',
      label: 'Usage data',
      to: settingsPath.usageData,
      icon: 'ChartNoAxesColumn',
    },
  ]
  return sections.filter((section) => section !== false)
}

/** The section a settings address belongs to. */
export function currentSettingsItem(
  nav: SettingsNavItem[],
  pathname: string,
): SettingsNavItem | undefined {
  return nav.find((item) => pathname === item.to || pathname.startsWith(`${item.to}/`))
}
