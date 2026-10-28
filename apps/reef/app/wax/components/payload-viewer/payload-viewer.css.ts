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

const frame = {
  background: theme.surface.onMainContentSubtle,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
}

export const body = style({
  ...theme.typography.code,
  color: theme.content.primary,
  margin: 0,
  minWidth: 0,
  padding: 12,
  tabSize: 2,
})
export const framedBody = style(frame)

export const actions = style({ alignItems: 'center', display: 'flex', gap: 2 })
/** Inline, the actions sit in a compact row above the card, at its right. */
export const inlineActions = style({ alignSelf: 'flex-end', marginBlockEnd: -4 })

/** Inline, a payload shows at most 320px, or half the screen on a short one, then scrolls
 * inside its card at the card's full width. Folding still sets how much there is to scroll. */
export const INLINE_MAX_HEIGHT = 'min(320px, 50vh)'
export const inlineBody = style({
  maxHeight: INLINE_MAX_HEIGHT,
  overflow: 'auto',
  selectors: {
    '&:focus-visible': { outline: `2px solid ${theme.stroke.focused}`, outlineOffset: -2 },
  },
})

/** In the expanded viewer, unwrapped lines scroll sideways. */
export const sideways = style({ overflowX: 'auto' })
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

export const moreButton = style({ marginInlineStart: 6 })

export const expandedPopup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  height: 'min(90vh, 1200px)',
})
