import classnames from 'classnames'
import { Activity, Loader, PanelLeft } from 'lucide-react'

import { customIcons, isCustomIcon } from '@/wax/components/icon/custom-icons/custom-icons'
import { iconContainer } from '@/wax/components/icon.css'

<<<<<<< HEAD
export type IconColor =
  | 'disabled'
  | 'error'
  | 'info'
  | 'inherit'
  | 'orange'
  | 'placeholder'
  | 'primary'
  | 'secondary'
  | 'success'
  | 'tertiary'
  | 'warning'
export type IconName =
  | 'Activity'
  | 'ArrowDown'
  | 'ArrowUp'
  | 'ChevronDown'
  | 'ChevronLeft'
  | 'ChevronRight'
  | 'CircleAlert'
  | 'Coral'
  | 'Loader'
  | 'Search'
  | 'X'

||||||| parent of 14a1f321 (fix(ui): move sidebar tooltip to toggle button)
export type IconColor = 'disabled' | 'error' | 'info' | 'inherit' | 'orange' | 'placeholder' | 'primary' | 'secondary' | 'success' | 'tertiary' | 'warning'
export type IconName = 'Activity' | 'ArrowDown' | 'ArrowUp' | 'ChevronDown' | 'ChevronLeft' | 'ChevronRight' | 'CircleAlert' | 'Coral' | 'Loader' | 'Search' | 'X'
=======
export type IconColor = 'disabled' | 'error' | 'info' | 'inherit' | 'orange' | 'placeholder' | 'primary' | 'secondary' | 'success' | 'tertiary' | 'warning'
export type IconName = 'Activity' | 'ArrowDown' | 'ArrowUp' | 'ChevronDown' | 'ChevronLeft' | 'ChevronRight' | 'CircleAlert' | 'Coral' | 'Loader' | 'PanelLeft' | 'Search' | 'X'
>>>>>>> 14a1f321 (fix(ui): move sidebar tooltip to toggle button)
export interface IconProps {
  className?: string
  color?: IconColor
  name: IconName
  size?: '14' | '16' | '18' | '20' | '22' | '24' | '30'
}
export type IconSize = '14' | '16' | '18' | '20' | '24' | '30'

const lucideIcons = {
  Activity,
  Loader,
  PanelLeft,
} as const

export function Icon({ className, color = 'primary', name, size = '20' }: IconProps) {
  const IconComponent = isCustomIcon(name) ? customIcons[name] : lucideIcons[name]
  return (
    <span className={classnames(iconContainer({ color, size: normalizeSize(size) }), className)}>
      <IconComponent color="currentColor" size={Number(size)} />
    </span>
  )
}

function normalizeSize(size: IconProps['size']): IconSize {
  if (size === '22') return '20'
  if (size === '24') return '24'
  return size as IconSize
}
