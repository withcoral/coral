import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const fields = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
})

export const field = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
})

export const label = style({
  color: theme.content.primary,
  fontWeight: 500,
})
