import { style } from '../../css'

import { theme } from '../../theme/theme.css'

export const container = style({
  alignItems: 'center',
  color: theme.content.tertiary,
  display: 'inline-flex',
  gap: '2px',
  ...theme.typography.bodySmall,
})
