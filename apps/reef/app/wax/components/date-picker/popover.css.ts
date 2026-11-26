import { style } from '../../css'

import { theme } from '../../theme/theme.css'

export const popover = style({
  backgroundColor: theme.surface.floating,
  border: `1px solid ${theme.stroke.floating}`,
  borderRadius: '10px',
  boxShadow: theme.elevation.e3,
  outline: 'none',
  padding: '4px',
})
