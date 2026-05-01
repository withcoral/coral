import { style } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'

export const root = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  padding: 16,
})

export const icon = style({
  color: theme.content.error,
})

export const details = style({
  color: theme.content.tertiary,
})
