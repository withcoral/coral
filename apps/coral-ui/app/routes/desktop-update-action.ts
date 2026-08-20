import type { Route } from './+types/desktop-update-action'

import { coralDesktopApi, desktopErrorMessage } from '@/lib/coral-desktop'
import { addToast } from '@/wax/components/toast'

// Downloading and installing are host requests, so they run in a client action
// rather than from the sidebar. The submitting fetcher is also what keeps the
// button disabled: installing quits Coral, so that submission never settles and
// the button stays disabled for the rest of the window's life.
export async function clientAction({ request }: Route.ClientActionArgs) {
  const desktop = coralDesktopApi()
  if (!desktop) throw new Response('Desktop bridge unavailable.', { status: 503 })

  const intent = (await request.formData()).get('intent')

  try {
    switch (intent) {
      case 'download':
        await desktop.downloadUpdate()
        break
      case 'install':
        await desktop.installUpdate()
        break
      default:
        throw new Response('Unsupported update action.', { status: 400 })
    }
  } catch (reason) {
    if (reason instanceof Response) throw reason

    addToast('error', {
      description: desktopErrorMessage(reason),
      title: intent === 'install' ? 'Couldn’t install the update' : 'Couldn’t download the update',
    })
  }

  return null
}

// Only the Desktop renderer can reach the host, so a browser posting here gets
// nothing rather than a server error.
export function action() {
  throw new Response('Not found', { status: 404 })
}
