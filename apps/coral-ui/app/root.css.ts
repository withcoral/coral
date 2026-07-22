import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const unavailable = style({
  background: theme.surface.mainContent,
  display: 'flex',
  minHeight: '100dvh',
})
