import { useAtomValue } from 'jotai'
import { useFetcher } from 'react-router'

import type { DesktopUpdateState } from '@/lib/coral-desktop'
import { desktopUpdateStateAtom } from '@/lib/desktop-update'
import { routePath } from '@/routing/routemap'

import type { DesktopUpdateIndicatorProps } from './desktop-update-indicator'

type DesktopUpdateStateResult = Pick<
  DesktopUpdateIndicatorProps,
  'isPending' | 'onDownload' | 'onInstall'
> & { state: DesktopUpdateState }

export function useDesktopUpdateState(): DesktopUpdateStateResult {
  const state = useAtomValue(desktopUpdateStateAtom)
  const fetcher = useFetcher()
  const action = routePath('desktopUpdate')
  const submit = (intent: 'download' | 'install') => {
    void fetcher.submit({ intent }, { action, method: 'post' })
  }

  return {
    // The main process publishes `downloading` a round trip later, so the
    // in-flight submission is what keeps the button disabled meanwhile.
    isPending: fetcher.state !== 'idle',
    onDownload: () => submit('download'),
    onInstall: () => submit('install'),
    state,
  }
}
