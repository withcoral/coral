import { SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import { Tooltip } from '@/wax/components/tooltip'

interface NavItemProps {
  children: string
  icon: React.ComponentProps<typeof SidebarButton>['icon']
  isActive: boolean
  isMinimized: boolean
  onClick: () => void
  variant?: React.ComponentProps<typeof SidebarButton>['variant']
}

export function NavItem({ children, icon, isActive, isMinimized, onClick, variant }: NavItemProps) {
  const button = (
    <SidebarButton
      icon={icon}
      isActive={isActive}
      isMinimized={isMinimized}
      onClick={onClick}
      variant={variant}
    >
      {!isMinimized && children}
    </SidebarButton>
  )

  if (isMinimized) {
    return <Tooltip content={children} side="right">{button}</Tooltip>
  }

  return button
}
