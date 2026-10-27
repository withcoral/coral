import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import {
  createContext,
  useLayoutEffect,
  useRef,
  useState,
  useSyncExternalStore,
  type RefObject,
} from 'react'

import { docked as dockedQuery, dock as dockClass, dockActive, mobile } from './dialog.css'

/** A modal over the page, a dialog floating beside it, or a panel in its layout. */
export type RailMode = 'modal' | 'beside' | 'docked'

interface RailState {
  mode: RailMode
  popup: RefObject<HTMLDivElement | null>
  /** Where a docked rail portals its popup: `null` until it mounts, `undefined` without one. */
  dock: HTMLElement | null | undefined
}

/** Set inside a `Rail`: its `Portal`, `Backdrop` and `Popup` follow the rail's mode. */
export const RailContext = createContext<RailState | null>(null)

/**
 * Set inside a rail beside the page. A nested dialog shares its parent's backdrop,
 * but such a rail has none, so a dialog opened from it draws its own.
 */
export const ForceBackdropContext = createContext(false)

const subscribe = (change: () => void) => {
  const queries = [window.matchMedia(mobile), window.matchMedia(dockedQuery)]
  for (const query of queries) query.addEventListener('change', change)
  return () => {
    for (const query of queries) query.removeEventListener('change', change)
  }
}

const viewportMode = (dockable: boolean): RailMode =>
  window.matchMedia(mobile).matches
    ? 'modal'
    : dockable && window.matchMedia(dockedQuery).matches
      ? 'docked'
      : 'beside'

export interface RailProps extends Omit<
  BaseDialog.Root.Props,
  'children' | 'disablePointerDismissal' | 'modal'
> {
  children: React.ReactNode
  /**
   * At 1200px or more, take a place in the page's layout instead of floating over
   * it, so the content beside it shrinks. Render the rail as an item of the page's
   * flex row, where the panel goes. Narrower, it floats as it does without this.
   */
  docked?: boolean
}

/**
 * A dialog the page stays usable beside, such as a record's detail next to its list.
 * Above the mobile width it has no backdrop, does not close on an outside press or
 * when focus leaves it, and neither traps focus nor locks scroll. Escape closes it
 * only from inside, so the page and its menus keep the key. At the mobile width it
 * is a full-screen modal. Crossing a width switches the mode while it is open and
 * keeps its content; a rail that turns modal takes focus.
 */
export function Rail({ children, onOpenChange, docked = false, ...props }: RailProps) {
  const mode = useSyncExternalStore(
    subscribe,
    () => viewportMode(docked),
    (): RailMode => 'beside',
  )
  const modal = mode === 'modal'
  const [dock, setDock] = useState<HTMLDivElement | null>(null)
  const popup = useRef<HTMLDivElement>(null)
  const wasModal = useRef(modal)
  useLayoutEffect(() => {
    const element = popup.current
    if (modal && !wasModal.current && element && !element.contains(document.activeElement))
      element.focus({ preventScroll: true })
    wasModal.current = modal
  }, [modal])
  return (
    <>
      {/* Every mode portals here, so crossing a width moves no content. */}
      {docked && <div ref={setDock} className={mode === 'docked' ? dockActive : dockClass} />}
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
        <RailContext value={{ mode, popup, dock: docked ? dock : undefined }}>
          {children}
        </RailContext>
      </BaseDialog.Root>
    </>
  )
}
