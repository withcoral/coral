import type { ButtonSize, ButtonVariant } from '@/wax/components/button/button.css'
import { Icon as BaseIcon } from '@/wax/components/icon'
import type { IconName, IconSize } from '@/wax/components/icon'

import { iconContainer } from './icon.css'

interface ImplementationProps extends Props {
  size: ButtonSize
  variant: ButtonVariant
}

interface Props {
  name: IconName
}

export function Icon(_props: Props) {
  return null
}

export function IconImplementation({ name, size, variant }: ImplementationProps) {
  return (
    <BaseIcon
      className={iconContainer({ buttonVariant: variant, size })}
      color="inherit"
      name={name}
      size={getIconSize(size)}
    />
  )
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
    default:
      return '18'
  }
}
