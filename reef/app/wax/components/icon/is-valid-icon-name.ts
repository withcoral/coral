import { icons } from 'lucide-react'

import { customIcons } from '@/wax/components/icon/custom-icons/custom-icons'

import type { IconName } from './icon'

export function isValidIconName(name: string): name is IconName {
  return name in icons || name in customIcons
}
