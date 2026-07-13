import classNames from 'classnames'

import type { ButtonSize, ButtonVariant } from '@/wax/components/button/button.css'
import { Icon as BaseIcon } from '@/wax/components/icon'
import type { IconName, IconSize } from '@/wax/components/icon'

import { iconContainer, spinning } from './icon.css'

interface ImplementationProps extends Props {
  className?: string
  size: ButtonSize
  variant: ButtonVariant
}

interface Props {
  name: IconName
}

export function Icon(_props: Props) {
  return null
}

export function SpinningButtonIcon(_props: Props) {
  return null
}

export function IconImplementation({ className, name, size, variant }: ImplementationProps) {
  return (
    <BaseIcon
      className={classNames(iconContainer({ buttonVariant: variant, size }), className)}
      color="inherit"
      name={name}
      size={getIconSize(size)}
    />
  )
}

export function SpinningIconImplementation(props: ImplementationProps) {
  return <IconImplementation {...props} className={spinning} />
}

export function LonelyIconImplementation({ name, size, variant }: ImplementationProps) {
  return (
    <BaseIcon
      className={iconContainer({ buttonVariant: variant, size })}
      color="inherit"
      name={name}
      size={getIconSize(size)}
    />
  )
}

function getIconSize(size: ButtonSize): IconSize {
  switch (size) {
    case '22':
      return '16'
    case '32':
      return '18'
    case '36':
      return '20'
  }
}
