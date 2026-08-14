import { redirect } from 'react-router'

import { routePath } from '@/routing/routemap'

// MCP Clients is useful in both Desktop and web builds. Desktop manages local
// client configuration; web offers bounded installer commands.
export function loader() {
  return redirect(routePath('settingsMcpClients'))
}
