import { Link, type To } from 'react-router'

import type { IconName } from '@/wax/components/icon'
import { Tooltip, type TooltipProps } from '@/wax/components/tooltip'

import type { ButtonSize, ButtonVariant } from './button.css'
import { Container } from './container'
import { Icon } from './icon'

/**
 * A link containing only an icon, for navigation IconButton cannot do. The
 * tooltip text is also its accessible name, so it always says where it goes.
 */
export function IconLink({
  name,
  to,
  tooltipText,
  tooltipSide,
  size = '32',
  variant = 'bare',
}: {
  name: IconName
  to: To
  tooltipText: string
  tooltipSide?: TooltipProps['side']
  size?: ButtonSize
  variant?: ButtonVariant
}) {
  return (
    <Tooltip content={tooltipText} side={tooltipSide}>
      <Container as={Link} to={to} ariaLabel={tooltipText} size={size} variant={variant}>
        <Icon name={name} />
      </Container>
    </Tooltip>
  )
}
