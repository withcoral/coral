import { ArrowDownIcon } from '@/wax/components/icon/custom-icons/arrow-down'
import { ArrowUpIcon } from '@/wax/components/icon/custom-icons/arrow-up'
import { ChevronDownIcon } from '@/wax/components/icon/custom-icons/chevron-down'
import { ChevronRightIcon } from '@/wax/components/icon/custom-icons/chevron-right'
import { CircleAlertIcon } from '@/wax/components/icon/custom-icons/circle-alert'
import { CoralIcon } from '@/wax/components/icon/custom-icons/coral'
import { PanelLeftIcon } from '@/wax/components/icon/custom-icons/panel-left'
import { SearchIcon } from '@/wax/components/icon/custom-icons/search'
import { XIcon } from '@/wax/components/icon/custom-icons/x'
import { IconName } from '@/wax/components/icon'

/**
 * How to add a custom icon:
 * - Go to Figma, copy the 24px icon. Lucide icon is based on 24px, so it MUST be that size.
 * - If it's not, Claude Code will be able to fix it later.
 * - Paste it into https://studio.lucide.dev/, click "Share > Copy React Code" from the topbar.
 * - Create new file, paste that, fix it up.
 */
export const customIcons = {
  ArrowDown: ArrowDownIcon,
  ArrowUp: ArrowUpIcon,
  ChevronDown: ChevronDownIcon,
  ChevronRight: ChevronRightIcon,
  CircleAlert: CircleAlertIcon,
  Coral: CoralIcon,
  PanelLeft: PanelLeftIcon,
  Search: SearchIcon,
  X: XIcon,
} as const

export type CustomIconName = keyof typeof customIcons

// Assuming we won't have name clashes. Can revisit in the future if that'll be the case
// E.g. if we'll need our own custom "Menu" icon, which clashes with the existing Lucide "Menu" icon
export function isCustomIcon(name: IconName): name is CustomIconName {
  return name in customIcons
}
