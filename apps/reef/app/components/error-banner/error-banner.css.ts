import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const banner = style({
  alignItems: 'center',
  background: theme.pill.red.background,
  border: `1px solid ${theme.pill.red.stroke}`,
  borderRadius: 10,
  color: theme.pill.red.color,
  display: 'flex',
  gap: 12,
  paddingBlock: 10,
  paddingInline: 12,
})

export const text = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 2,
  minWidth: 0,
})
