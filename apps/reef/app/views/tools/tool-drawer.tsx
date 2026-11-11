import type { ReactNode } from 'react'

import type { ProviderLogo } from '@/views/providers/types'
import { Button, Dialog, Menu, Typography } from '@/wax/components'
import { CodeChip } from '@/wax/components/code-chip'
import type { IconName } from '@/wax/components/icon'
import { Logo } from '@/wax/components/logo'

import * as styles from './tools.css'

/**
 * The top of the tool drawer, the same for every kind of tool: the group the
 * tool belongs to above its name, then the tool's own actions and the close.
 */
export function ToolDrawerHeader({
  group,
  title,
  actions,
}: {
  /** Names the group above the title, such as "Shared tool", with its logo. */
  group: { label: string; logo: string; icons?: ProviderLogo }
  title: string
  /** The tool's actions: its main one, then its menu. */
  actions?: ReactNode
}) {
  return (
    <div className={styles.drawerHeader}>
      <div className={styles.drawerName}>
        <span className={styles.drawerGroup}>
          <Logo name={group.logo} icons={group.icons} size="20" />
          <Typography.BodySmall as="span" variant="secondary">
            {group.label}
          </Typography.BodySmall>
        </span>
        <Dialog.Title>{title}</Dialog.Title>
      </div>
      <div className={styles.drawerHeaderActions}>
        {actions}
        <Dialog.Close inline />
      </div>
    </div>
  )
}

/** The tool's call path, to copy, and one line on where it comes from. */
export function ToolIdentity({
  callPath,
  provenance,
}: {
  callPath: string
  provenance?: ReactNode
}) {
  return (
    <div className={styles.identity}>
      <div className={styles.callPath}>
        <CodeChip>{callPath}</CodeChip>
        <Button.CopyButton
          textToCopy={callPath}
          ariaLabel="Copy call path"
          size="22"
          variant="bare"
        />
      </div>
      {provenance && (
        <Typography.BodySmall as="p" variant="secondary">
          {provenance}
        </Typography.BodySmall>
      )}
    </div>
  )
}

/** The tool's less common actions, such as its removal, in a menu at the header's end. */
export function ToolMenu({
  items,
  trigger,
}: {
  items: { label: string; icon: IconName; onSelect: () => void }[]
  /** Where focus returns when a dialog an item opens closes. */
  trigger: React.Ref<HTMLButtonElement>
}) {
  return (
    <Menu.Container>
      <Menu.Trigger
        render={
          <Button.IconButton
            ref={trigger}
            name="Ellipsis"
            ariaLabel="More actions"
            variant="bare"
          />
        }
      >
        {null}
      </Menu.Trigger>
      <Menu.Content>
        {items.map((item) => (
          <Menu.Item key={item.label} icon={item.icon} onClick={item.onSelect}>
            {item.label}
          </Menu.Item>
        ))}
      </Menu.Content>
    </Menu.Container>
  )
}
