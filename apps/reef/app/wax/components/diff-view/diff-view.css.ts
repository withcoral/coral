import { style, styleVariants } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

const PADDING = 8

/** The frame keeps still: only the lines inside scroll. Its line height is the code's, so `lh` counts lines. */
export const frame = style({
  ...theme.typography.code,
  background: theme.surface.onMainContentSubtle,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  flexShrink: 0,
  minWidth: 0,
})
export const FRAME_PADDING = PADDING

// Both line numbers and the marker share their columns down the whole diff.
// The rows are as wide as the longest line, so a row's tint reaches the end
// of the line when the diff scrolls sideways.
export const rows = style({
  color: theme.content.primary,
  display: 'grid',
  gridTemplateColumns: 'max-content max-content max-content 1fr',
  minWidth: 'max-content',
  paddingBlock: PADDING,
})

const row = style({
  display: 'grid',
  gridColumn: '1 / -1',
  gridTemplateColumns: 'subgrid',
})

export const line = styleVariants({
  same: [row],
  removed: [row, { background: theme.content.errorBackground }],
  added: [row, { background: theme.content.successBackground }],
})

export const number = style({
  color: theme.content.tertiary,
  paddingInline: `${PADDING}px 4px`,
  textAlign: 'end',
  userSelect: 'none',
})

/** The sign says the change without its colour. */
export const marker = styleVariants({
  same: { paddingInline: 8, userSelect: 'none' },
  removed: { color: theme.content.error, paddingInline: 8, userSelect: 'none' },
  added: { color: theme.content.success, paddingInline: 8, userSelect: 'none' },
})

export const text = style({
  font: 'inherit',
  paddingInlineEnd: PADDING,
  tabSize: 2,
  whiteSpace: 'pre',
})

export const fold = style({
  color: theme.content.tertiary,
  gridColumn: '1 / -1',
  paddingBlock: 2,
  paddingInline: PADDING,
  userSelect: 'none',
})
