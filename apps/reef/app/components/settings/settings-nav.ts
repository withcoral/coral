import type { IconName } from '@/wax/components/icon'

/**
 * The organization's settings: what an organization owner administers for
 * everyone, away from the work in a workspace. This module lists the sections
 * of the settings nav and who sees each. `routes.ts` puts each section's page
 * under the settings layout.
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
  secretStoresEnabled: boolean
}

/**
 * The organization's sections for this reader, in the order they show. Only an
 * organization owner has any: Coral refuses these pages to everyone else. The
 * account menu's Settings opens the first.
 */
export function settingsNav({
  organizationOwner,
  people,
  secretStoresEnabled,
}: SettingsReader): SettingsNavItem[] {
  if (!organizationOwner) return []
  const sections: (SettingsNavItem | false)[] = [
    people && {
      id: 'members',
      label: 'Members',
      to: settingsPath.organizationMembers,
      icon: 'Users',
    },
    {
      id: 'workspaces',
      label: 'Workspaces',
      to: settingsPath.organizationWorkspaces,
      icon: 'Folders',
    },
    secretStoresEnabled && {
      id: 'secret-stores',
      label: 'Secret stores',
      to: settingsPath.secretStores,
      icon: 'KeyRound',
    },
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
