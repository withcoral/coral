import { redirect } from 'react-router'

import { routePath } from '@/routing/routemap'

// Settings has one page today, so the section index sends every visitor to it.
export function loader() {
  return redirect(routePath('settingsMcpClients'))
}
