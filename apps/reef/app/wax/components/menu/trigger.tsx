import { Menu as BaseMenu } from '@base-ui/react/menu'
import classNames from 'classnames'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'

import * as styles from './menu.css'

export interface TriggerProps {
  disabled?: boolean
  children: React.ReactNode
  className?: string
  /** The trigger's id. Base UI generates one otherwise, and it wins over the render element's. */
  id?: string
  render?: React.ReactElement<Record<string, unknown>>
}

export function Trigger({ children, className, disabled, id, render }: TriggerProps) {
  const nativeButton =
    !render ||
    render.type === 'button' ||
    ((render.type === ButtonContainer || render.type === SidebarButton) &&
      (!render.props.as || render.props.as === 'button'))
  return (
    <BaseMenu.Trigger
      disabled={disabled}
      id={id}
      className={classNames(styles.trigger, className)}
      nativeButton={nativeButton}
      render={render}
    >
      {children}
    </BaseMenu.Trigger>
  )
}
