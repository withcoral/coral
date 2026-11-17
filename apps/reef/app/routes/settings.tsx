import { Outlet } from 'react-router'

/**
 * The layout of the organization's settings. It renders nothing of its own:
 * matching it is what marks a page as settings, so the shell shows the settings
 * nav in place of the workspace's. A page joins the settings by sitting under
 * this route in `routes.ts`, whatever its path.
 */
export const handle = { area: 'settings' as const }

export default function Settings() {
  return <Outlet />
}
