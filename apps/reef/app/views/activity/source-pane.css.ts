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
export const location = style({
  display: 'flex',
  alignItems: 'center',
  gap: 4,
  color: theme.content.tertiary,
  ...theme.typography.bodySmall,
})
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
/** The pane's actions, compact, at its header's right. */
export const actions = style({
  display: 'flex',
  alignItems: 'center',
  gap: 2,
  marginInlineStart: 'auto',
})
/** Expanded, the source fills the dialog under its header and scrolls inside it. */
export const expandedCode = style({
  display: 'flex',
  flexDirection: 'column',
  flex: 1,
  minHeight: 0,
})
