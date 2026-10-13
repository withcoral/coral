import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import { createContext, useCallback, useLayoutEffect, useRef, useSyncExternalStore } from 'react'

import { mobile } from './dialog.css'

interface BesidePage {
  /** Wide enough to leave the page usable: no backdrop, on its own layer. */
  beside: boolean
  /** The popup passes its element, so the root can focus it. */
  register: (popup: HTMLDivElement | null) => void
}

/** Set inside a `BesidePageRoot`: its `Backdrop` and `Popup` follow its mode. */
export const BesidePageContext = createContext<BesidePage | null>(null)

/**
 * Set inside a drawer beside the page. A dialog nested in another shares its
 * backdrop, but this drawer has none, so a dialog opened from it draws its own.
 */
export const OwnBackdropContext = createContext(false)

const subscribe = (change: () => void) => {
  const query = window.matchMedia(mobile)
  query.addEventListener('change', change)
  return () => query.removeEventListener('change', change)
}

export interface BesidePageRootProps extends Omit<
  BaseDialog.Root.Props,
  'children' | 'disablePointerDismissal' | 'modal'
> {
  children: React.ReactNode
}

/**
 * A drawer the page stays usable beside. Above the mobile width it has no
 * backdrop, does not close on an outside press or when focus leaves it, and
 * neither traps focus nor locks scroll. Escape still closes it from anywhere
 * except a menu or dialog, which keeps the key.
 * At the mobile width it is a full-screen modal. Crossing the width switches
 * the mode while open, and a drawer that becomes modal takes focus.
 */
export function BesidePageRoot({ children, ...props }: BesidePageRootProps) {
  const modal = useSyncExternalStore(
    subscribe,
    () => window.matchMedia(mobile).matches,
    () => false,
  )
  const popup = useRef<HTMLDivElement>(null)
  const register = useCallback((element: HTMLDivElement | null) => {
    popup.current = element
  }, [])
  const wasModal = useRef(modal)
  useLayoutEffect(() => {
    const element = popup.current
    if (modal && !wasModal.current && element && !element.contains(document.activeElement))
      element.focus({ preventScroll: true })
    wasModal.current = modal
  }, [modal])
  return (
    <BaseDialog.Root {...props} modal={modal} disablePointerDismissal={!modal}>
      <BesidePageContext value={{ beside: !modal, register }}>{children}</BesidePageContext>
    </BaseDialog.Root>
  )
}
