import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import { createContext, useContext, useState, type ReactNode } from 'react'
import { Button, Dialog } from '@/wax/components'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import * as styles from './navigation-frame.css'

interface Drawer {
  handle: BaseDialog.Handle<unknown>
  /** The mounted trigger: the current page's, since each page brings its own. */
  trigger: () => HTMLButtonElement | null
  attachTrigger: (element: HTMLButtonElement | null) => void
}

const DrawerContext = createContext<Drawer | null>(null)

/** Lets a page's `NavigationTrigger` open the drawer of the `NavigationFrame` beside it. */
export function NavigationRoot({ children }: { children: ReactNode }) {
  const [drawer] = useState<Drawer>(() => {
    let trigger: HTMLButtonElement | null = null
    return {
      handle: BaseDialog.createHandle(),
      trigger: () => trigger,
      attachTrigger: (element) => {
        trigger = element
      },
    }
  })
  return <DrawerContext value={drawer}>{children}</DrawerContext>
}

/**
 * Persistent desktop navigation, or a focus-managed drawer on a phone. The
 * drawer opens from the `NavigationTrigger` in the page's top bar; both sit
 * inside one `NavigationRoot`.
 */
export function NavigationFrame({
  navigationKey,
  children,
}: {
  navigationKey: string
  children: (mobile: boolean) => ReactNode
}) {
  const drawer = useContext(DrawerContext)
  const [openedAt, setOpenedAt] = useState<string | null>(null)
  // Clear rather than merely hide on navigation: Back must not reopen an old drawer.
  if (openedAt !== null && openedAt !== navigationKey) setOpenedAt(null)
  return (
    <>
      <div className={styles.desktop}>{children(false)}</div>
      <Dialog.Root
        handle={drawer?.handle}
        open={openedAt === navigationKey}
        onOpenChange={(open) => setOpenedAt(open ? navigationKey : null)}
      >
        <Dialog.Portal>
          <Dialog.Backdrop />
          {/* Navigation mounts a new page, and its trigger: focus returns to that one. */}
          <Dialog.Popup placement="left" className={styles.drawer} finalFocus={drawer?.trigger}>
            <Dialog.Title className={visuallyHidden}>Navigation</Dialog.Title>
            {children(true)}
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
    </>
  )
}

/** Opens the navigation drawer. It shows only on a phone, inside a `NavigationRoot`. */
export function NavigationTrigger() {
  const drawer = useContext(DrawerContext)
  if (!drawer) return null
  const { handle, attachTrigger } = drawer
  return (
    <span className={styles.mobile}>
      <Dialog.Trigger
        handle={handle}
        render={
          <Button.IconButton
            ref={attachTrigger}
            name="PanelLeft"
            ariaLabel="Open navigation"
            variant="bare"
          />
        }
      >
        {null}
      </Dialog.Trigger>
    </span>
  )
}

/** Closes the navigation drawer, from where the sidebar collapses on a desktop. */
export function NavigationClose() {
  return (
    <BaseDialog.Close
      render={
        <Button.IconButton name="PanelLeft" ariaLabel="Close navigation" size="32" variant="bare" />
      }
    />
  )
}
