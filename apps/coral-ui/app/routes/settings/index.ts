import { redirect } from 'react-router'

import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { routePath } from '@/routing/routemap'

// MCP Clients was the settings landing page before settings gained sub-pages,
// and it exists only in the Desktop shell. Everywhere else, runtime features are
// the first page a browser can actually use.
export function loader() {
  return redirect(
    isCoralDesktopBuild() ? routePath('settingsMcpClients') : routePath('settingsRuntimeFeatures'),
  )
}
