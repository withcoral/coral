import { style } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

export const viewer = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
})

/** The expanded viewer fills its dialog and scrolls its own body. */
export const expandedViewer = style({ flex: 1, minHeight: 0 })

export const toolbar = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  minWidth: 0,
})

export const facts = style({ flexGrow: 1, minWidth: 0 })

export const find = style({
  alignItems: 'center',
  display: 'flex',
  gap: 4,
  minWidth: 0,
  flexBasis: 220,
  flexGrow: 1,
  maxWidth: 360,
})

export const findInput = style({ flex: 1, minWidth: 0 })

export const body = style({
  ...theme.typography.code,
  background: theme.surface.onMainContentSubtle,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  color: theme.content.primary,
  margin: 0,
  minWidth: 0,
  padding: 12,
  tabSize: 2,
})

/** Unwrapped lines scroll sideways here; the detail pane still owns vertical scroll. */
export const sideways = style({ overflowX: 'auto', overflowY: 'hidden' })
export const expandedBody = style({ flex: 1, minHeight: 0, overflow: 'auto' })

export const wrapped = style({ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' })
export const unwrapped = style({ whiteSpace: 'pre' })

export const line = style({ display: 'flex', alignItems: 'baseline', gap: 2, minWidth: 0 })
export const lineText = style({ minWidth: 0 })
export const raw = style({ margin: 0, minWidth: 0 })

export const children = style({ paddingInlineStart: 18 })

/** A container's toggle sits in the indent, so keys stay aligned with their siblings. */
export const toggle = style({ flexShrink: 0, marginInlineStart: -4 })
export const toggleSpacer = style({ display: 'inline-block', flexShrink: 0, width: 18 })

export const summary = style({ color: theme.content.tertiary })

export const numbered = style({
  display: 'grid',
  gridTemplateColumns: 'max-content minmax(0, 1fr)',
  columnGap: 12,
})
export const lineNumber = style({
  color: theme.content.tertiary,
  textAlign: 'end',
  userSelect: 'none',
})

// A match is heavier and tinted, not tinted alone; the current one is also outlined.
export const mark = style({
  background: theme.content.warningBackground,
  borderRadius: 2,
  color: 'inherit',
  fontWeight: theme.typography.bodySmallXStrong.fontWeight,
})
export const currentMark = style({
  outline: `2px solid ${theme.stroke.focused}`,
  outlineOffset: 1,
})

export const moreButton = style({ marginInlineStart: 6 })

export const expandedPopup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  height: 'min(90vh, 1200px)',
})
