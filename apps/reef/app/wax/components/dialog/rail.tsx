import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import { createContext, useLayoutEffect, useRef, useSyncExternalStore, type RefObject } from 'react'

import { mobile } from './dialog.css'

interface RailMode {
  /** At the mobile width the rail is a modal; beside the page it is not. */
  modal: boolean
  popup: RefObject<HTMLDivElement | null>
}

/** Set inside a `Rail`: its `Backdrop` and `Popup` follow the rail's mode. */
export const RailContext = createContext<RailMode | null>(null)

/**
 * Set inside a rail beside the page. A nested dialog shares its parent's backdrop,
 * but such a rail has none, so a dialog opened from it draws its own.
 */
export const ForceBackdropContext = createContext(false)

const subscribe = (change: () => void) => {
  const query = window.matchMedia(mobile)
  query.addEventListener('change', change)
  return () => query.removeEventListener('change', change)
}

export interface RailProps extends Omit<
  BaseDialog.Root.Props,
  'children' | 'disablePointerDismissal' | 'modal'
> {
  children: React.ReactNode
}

/**
 * A dialog the page stays usable beside, such as a record's detail next to its list.
 * Above the mobile width it has no backdrop, does not close on an outside press or
 * when focus leaves it, and neither traps focus nor locks scroll. Escape closes it
 * only from inside, so the page and its menus keep the key. At the mobile width it
 * is a full-screen modal. Crossing the width switches the mode while it is open; a
 * rail that turns modal takes focus.
 */
export function Rail({ children, onOpenChange, ...props }: RailProps) {
  const modal = useSyncExternalStore(
    subscribe,
    () => window.matchMedia(mobile).matches,
    () => false,
  )
  const popup = useRef<HTMLDivElement>(null)
  const wasModal = useRef(modal)
  useLayoutEffect(() => {
    const element = popup.current
    if (modal && !wasModal.current && element && !element.contains(document.activeElement))
      element.focus({ preventScroll: true })
    wasModal.current = modal
  }, [modal])
  return (
    <BaseDialog.Root
      {...props}
      modal={modal}
      disablePointerDismissal={!modal}
      onOpenChange={(open, details) => {
        if (
          !modal &&
          details.reason === 'escape-key' &&
          !popup.current?.contains(details.event.target as Node | null)
        )
          return details.cancel()
        onOpenChange?.(open, details)
      }}
    >
      <RailContext value={{ modal, popup }}>{children}</RailContext>
    </BaseDialog.Root>
  )
}
