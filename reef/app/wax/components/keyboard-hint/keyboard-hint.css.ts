import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const container = style({
  alignItems: 'center',
  color: theme.content.tertiary,
  display: 'inline-flex',
  gap: '2px',
  ...theme.typography.bodySmall,
})
