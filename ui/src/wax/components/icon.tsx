import classnames from 'classnames'
import { Activity, ArrowDown, ArrowUp, ChevronDown, ChevronRight, CircleAlert, Columns3, Database, Loader, PanelLeft, Play, Plus, RefreshCw, Search, Table2, X } from 'lucide-react'

import { CoralIcon } from '@/wax/components/icon/custom-icons/coral'
import { iconContainer } from '@/wax/components/icon.css'

export type IconColor = 'disabled' | 'error' | 'info' | 'inherit' | 'orange' | 'placeholder' | 'primary' | 'secondary' | 'success' | 'tertiary' | 'warning'
export type IconName = 'Coral' | 'PanelLeft' | 'Database' | 'Search' | 'X' | 'ChevronDown' | 'ChevronRight' | 'ArrowUp' | 'ArrowDown' | 'Table2' | 'Columns3' | 'Plus' | 'Play' | 'Loader' | 'RefreshCw' | 'CircleAlert' | 'Activity'
export interface IconProps {
  className?: string
  color?: IconColor
  name: IconName
  size?: '14' | '16' | '18' | '20' | '22' | '24' | '30'
}
export type IconSize = '14' | '16' | '18' | '20' | '24' | '30'

const iconMap = {
  Activity,
  PanelLeft,
  Database,
  Search,
  X,
  ChevronDown,
  ChevronRight,
  ArrowUp,
  ArrowDown,
  Table2,
  Columns3,
  Plus,
  Play,
  Loader,
  RefreshCw,
  CircleAlert,
} as const

export function Icon({ className, color = 'primary', name, size = '20' }: IconProps) {
  const IconComponent = name === 'Coral' ? CoralIcon : iconMap[name]
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
