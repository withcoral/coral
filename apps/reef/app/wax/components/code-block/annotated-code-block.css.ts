import { style } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

export const code = style({
  margin: 0,
  minWidth: 0,
})
/** A test and layout hook onto the scrollable viewport Base UI renders. */
export const viewport = style({})
export const line = style({
  display: 'flex',
  gap: 12,
  paddingInline: 4,
  borderLeft: '3px solid transparent',
  whiteSpace: 'pre',
})
export const lineHighlighted = style({
  background: theme.surface.onMainContentSubtle,
  borderLeftColor: theme.content.link,
})
export const gutter = style({
  flexShrink: 0,
  minWidth: 28,
  textAlign: 'right',
  color: theme.content.tertiary,
  userSelect: 'none',
})
export const lineText = style({
  minWidth: 0,
})
