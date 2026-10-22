import classnames from 'classnames'
import { icons } from 'lucide-react'

import { customIcons, isCustomIcon } from '@/wax/components/icon/custom-icons/custom-icons'
import type { CustomIconName } from '@/wax/components/icon/custom-icons/custom-icons'

import { iconContainer, plate } from './icon.css'
import type { IconContainerVariants } from './icon.css'

export type IconColor = NonNullable<IconContainerVariants>['color']
export type IconName = CustomIconName | LucideIconName
export interface IconProps {
  className?: string
  color?: IconColor
  name: IconName
  size?: '14' | '16' | '18' | '20' | '24' | '30'
}
export type IconSize = NonNullable<IconContainerVariants>['size']
export type LucideIconName = keyof typeof icons

export function Icon({ className, color = 'primary', name, size = '20' }: IconProps) {
  const IconComponent = isCustomIcon(name) ? customIcons[name] : icons[name]

  return (
    <span className={classnames(iconContainer({ color, size }), className)}>
      <IconComponent color="currentColor" size={Number(size)} />
    </span>
  )
}

export type IconPlateTone = keyof typeof plate

/** An icon on a tinted plate, coloured like the pill that names the same state. */
export function IconPlate({ name, tone }: { name: IconName; tone: IconPlateTone }) {
  return (
    <span className={plate[tone]}>
      <Icon name={name} color="inherit" size="18" />
    </span>
  )
}
