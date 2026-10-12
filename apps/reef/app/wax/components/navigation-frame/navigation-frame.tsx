import { useState, type ReactNode } from 'react'
import { Button, Dialog, Typography } from '@/wax/components'
import * as styles from './navigation-frame.css'

/** Persistent desktop navigation, or a focus-managed drawer beneath a compact header. */
export function NavigationFrame({
  navigationKey,
  label,
  children,
}: {
  navigationKey: string
  label: string
  children: (mobile: boolean) => ReactNode
}) {
  const [openedAt, setOpenedAt] = useState<string | null>(null)
  // Clear rather than merely hide on navigation: Back must not reopen an old drawer.
  if (openedAt !== null && openedAt !== navigationKey) setOpenedAt(null)
  return (
    <>
      <div className={styles.desktop}>{children(false)}</div>
      <div className={styles.mobile}>
        <Dialog.Root
          open={openedAt === navigationKey}
          onOpenChange={(open) => setOpenedAt(open ? navigationKey : null)}
        >
          <header className={styles.header}>
            <Dialog.Trigger
              render={
                <Button.IconButton name="PanelLeft" ariaLabel="Open navigation" variant="bare" />
              }
            >
              {null}
            </Dialog.Trigger>
            <Typography.BodyStrong>{label}</Typography.BodyStrong>
          </header>
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup placement="left" className={styles.drawer}>
              <Dialog.Title>Navigation</Dialog.Title>
              <Dialog.Close />
              {children(true)}
            </Dialog.Popup>
          </Dialog.Portal>
        </Dialog.Root>
      </div>
    </>
  )
}
