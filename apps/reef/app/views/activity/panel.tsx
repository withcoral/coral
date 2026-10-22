import { createContext, useContext, useEffect, useState, type ReactNode } from 'react'
import { createPortal } from 'react-dom'
import classNames from 'classnames'
import { Typography } from '@/wax/components'
import * as Card from '@/wax/components/card'
import * as Dialog from '@/wax/components/dialog'
import { IconPlate, type IconName, type IconPlateTone } from '@/wax/components/icon'
import * as styles from './panel.css'

/** The drawer's close control. A panel's sticky header places it beside the status. */
export const PanelClose = createContext<ReactNode>(null)

/** True inside a pane that scrolls on its own: sticky edges meet that pane, not the
 * drawer's padded edge, so they do not offset the drawer's padding. Such a pane holds
 * the footer slot below its scroll area and provides it through `PanelFooterSlot`. */
export const PanelInset = createContext(false)

/** Where a panel's decision controls render. */
export const PanelFooterSlot = createContext<HTMLElement | null>(null)
/** How a Panel inside such a pane gives the pane its `label`: the pane holds both the
 * evidence and the footer slot, so the pane is the one region that the label names. */
export const PanelInsetLabel = createContext<(label: string | undefined) => void>(() => {})
const PanelLabel = createContext<string | undefined>(undefined)

/** The accessible name of the Panel around this content, if it has one. Content that names
 * itself the same leaves the name to the Panel, which also holds the pinned footer. */
export function usePanelLabel() {
  return useContext(PanelLabel)
}

/** One Activity detail panel. It ends with the one footer slot that stays pinned to the
 * drawer's bottom: a sticky footer pins only within its parent, so the slot spans the panel.
 * A `label` names the panel as a region, so its footer controls stay inside that region.
 * Inside a pane that scrolls on its own, the pane holds that slot instead. */
export function Panel({ label, children }: { label?: string; children: ReactNode }) {
  const [slot, setSlot] = useState<HTMLElement | null>(null)
  const close = useContext(PanelClose)
  const inset = useContext(PanelInset)
  const nameInset = useContext(PanelInsetLabel)
  useEffect(() => {
    if (!inset) return
    nameInset(label)
    return () => nameInset(undefined)
  }, [inset, label, nameInset])
  if (inset)
    return (
      <section className={styles.panel}>
        <PanelLabel value={label}>{children}</PanelLabel>
      </section>
    )
  return (
    <section className={styles.panel} aria-label={label}>
      <PanelFooterSlot value={slot}>
        <PanelLabel value={label}>{children}</PanelLabel>
      </PanelFooterSlot>
      <Card.Footer
        ref={setSlot}
        sticky
        className={classNames(
          styles.footerSlot,
          Boolean(close) && !inset && Dialog.pinnedBottomClassName,
        )}
      />
    </section>
  )
}

/** The panel's record identity: a status icon, the heading, and its status, aligned on
 * the heading's first line. Lines under the heading say more about the same record. */
export function PanelHeader({
  icon,
  tone,
  title,
  heading = 'h2',
  details,
  status,
  sticky = true,
}: {
  icon: IconName
  tone: IconPlateTone
  title: ReactNode
  heading?: 'h2' | 'h3'
  details?: ReactNode
  /** Text, never colour alone, such as a Pill. */
  status: ReactNode
  /** A sticky header leads the drawer and carries its close control; a nested one does not. */
  sticky?: boolean
}) {
  const close = useContext(PanelClose)
  const inset = useContext(PanelInset)
  return (
    <Card.Header
      sticky={sticky}
      className={classNames(
        styles.header,
        sticky && Boolean(close) && !inset && Dialog.pinnedClassName,
      )}
    >
      <IconPlate name={icon} tone={tone} />
      <div className={styles.summary}>
        <Typography.BodyLargeStrong as={heading} className={styles.title}>
          {title}
        </Typography.BodyLargeStrong>
        <div className={styles.details}>{details}</div>
      </div>
      <div className={styles.trailing}>
        {status}
        {sticky && close}
      </div>
    </Card.Header>
  )
}

/** A panel's decision controls, pinned to the drawer's bottom inside a Panel. Outside one,
 * and before the panel mounts, they render here in their own sticky footer. */
export function PanelFooter({ children }: { children: ReactNode }) {
  const slot = useContext(PanelFooterSlot)
  const close = useContext(PanelClose)
  const inset = useContext(PanelInset)
  if (slot) return createPortal(children, slot)
  return (
    <Card.Footer sticky className={close && !inset ? Dialog.pinnedBottomClassName : undefined}>
      {children}
    </Card.Footer>
  )
}
