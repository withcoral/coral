import { style } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'

export const root = style({
  display: 'flex',
  flexDirection: 'column',
  minHeight: 0,
  minWidth: 0,
  height: '100%',
  gap: 8,
})
export const header = style({
  display: 'flex',
  alignItems: 'center',
  gap: 12,
  flexShrink: 0,
})
export const follow = style({
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  marginInlineStart: 'auto',
  flexShrink: 0,
})
const note = {
  display: 'flex',
  alignItems: 'center',
  gap: 4,
  color: theme.content.tertiary,
  ...theme.typography.bodySmall,
} as const
export const readOnly = style(note)
export const location = style(note)
export const missing = style({
  display: 'flex',
  alignItems: 'center',
  gap: 6,
  flexShrink: 0,
  padding: '6px 10px',
  borderRadius: 6,
  background: theme.content.warningBackground,
  color: theme.content.warning,
})
