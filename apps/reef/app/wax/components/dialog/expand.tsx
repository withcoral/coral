import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import { useEffect, useEffectEvent, useState } from 'react'
import * as Button from '@/wax/components/button'
import { Backdrop } from './backdrop'
import { Close } from './close'
import { Header } from './header'
import { Popup } from './popup'
import { Title } from './title'
import * as styles from './expand.css'

/**
 * A compact Expand control that opens `children` in a tall dialog under Wax's standard
 * header: `label` as its title, and its close control at the top right. Closed, focus is
 * back on Expand.
 */
export function Expand({
  label,
  onExpandedChange,
  children,
}: {
  /** Names the Expand control and titles the dialog. */
  label: string
  /** Called with true when the dialog opens, and with false once it has closed and focus is
   * back on Expand, or when it unmounts open. A page can hold still meanwhile. */
  onExpandedChange?: (expanded: boolean) => void
  children: React.ReactNode
}) {
  const [open, setOpen] = useState(false)
  // From the open until the close has ended, so focus is back on Expand before a page moves.
  const [held, setHeld] = useState(false)
  const reportExpanded = useEffectEvent((next: boolean) => onExpandedChange?.(next))
  // The cleanup also runs when the control unmounts open, so no page stays held.
  useEffect(() => {
    if (!held) return
    reportExpanded(true)
    return () => reportExpanded(false)
  }, [held])
  return (
    <BaseDialog.Root
      open={open}
      onOpenChange={(next) => {
        setOpen(next)
        if (next) setHeld(true)
      }}
      onOpenChangeComplete={(next) => {
        // A close that ends after you opened it again leaves the hold.
        if (!next && !open) setHeld(false)
      }}
    >
      <BaseDialog.Trigger
        render={<Button.Container size="22" variant="bare" ariaLabel={`Expand ${label}`} />}
      >
        <Button.Icon name="Maximize2" />
      </BaseDialog.Trigger>
      <BaseDialog.Portal>
        <Backdrop />
        <Popup size="xxl" className={styles.popup}>
          <Header>
            <Title>{label}</Title>
            <Close />
          </Header>
          {children}
        </Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
  )
}
