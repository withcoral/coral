import { Menu as BaseMenu } from '@base-ui/react/menu'
import classNames from 'classnames'
import { Link, To } from 'react-router'

import { Icon } from '../icon'
import type { IconName } from '../icon'
import { Tooltip } from '../tooltip'

import * as styles from './menu.css'

export interface ItemProps {
  /** Names an item that shows only its icon, such as a row's one action. */
  ariaLabel?: string
  /** The label. Without one, the item shows only its icon and needs `ariaLabel`. */
  children?: React.ReactNode
  className?: string
  closeOnClick?: boolean
  disabled?: boolean
  icon?: IconName
  /** An external page, opened in a new tab. */
  href?: string
  onClick?: () => void
  to?: To
  /** Says what the item does when you point at it or reach it. An icon-only item needs one. */
  tooltip?: string
}

export function Item({
  ariaLabel,
  children,
  className,
  closeOnClick = true,
  disabled = false,
  href,
  icon,
  onClick,
  to,
  tooltip,
}: ItemProps) {
  const label = (
    <Tooltip content={children} showOnlyWhenTruncated>
      <span className={styles.itemLabel}>{children}</span>
    </Tooltip>
  )

  const content = (
    <>
      {icon && <Icon color="tertiary" name={icon} size="18" />}
      {children !== undefined && <div className={styles.itemContent}>{label}</div>}
    </>
  )
  const itemClass = classNames(styles.item, className, {
    [styles.itemIconOnly]: children === undefined,
  })

  // An external page opens in a new tab; a path opens in the app; anything else is a command.
  const render = href ? (
    <a href={href} target="_blank" rel="noreferrer" />
  ) : to ? (
    <Link to={to} />
  ) : undefined
  const item = (
    <BaseMenu.Item
      aria-label={ariaLabel}
      className={itemClass}
      closeOnClick={closeOnClick}
      disabled={disabled}
      onClick={onClick}
      render={render}
    >
      {content}
    </BaseMenu.Item>
  )
  return tooltip ? <Tooltip content={tooltip}>{item}</Tooltip> : item
}
