import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const layout = style({
  backgroundColor: theme.surface.main,
  display: 'flex',
  height: '100dvh',
  overflow: 'hidden',
})
