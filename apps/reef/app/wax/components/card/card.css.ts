import { style, styleVariants } from '@/wax/css'

import { utils } from '@/styles/utils'
import { theme } from '@/wax/theme/theme.css'

export const card = style({
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: '12px',
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
  padding: '16px',
  textDecoration: 'none',
  width: '100%',
})

// A row in a dense list of cards: the icon sets the height, with a small margin.
export const cardCompact = style({
  padding: '8px',
  paddingInlineEnd: '12px',
})

export const cardButton = style({
  alignItems: 'stretch',
  color: 'inherit',
  cursor: 'pointer',
  font: 'inherit',
  justifyContent: 'flex-start',
  textAlign: 'left',
  selectors: {
    '&:hover:not(:disabled)': {
      background: theme.surface.onMainContentHover,
    },
    '&:disabled': { cursor: 'default', opacity: 0.6 },
    '&:focus-visible': {
      outline: `2px solid ${theme.stroke.focused}`,
      outlineOffset: '2px',
    },
  },
})

export const header = style({
  alignItems: 'center',
  display: 'flex',
  gap: '10px',
  minWidth: 0,
})

export const stickyHeader = style({
  position: 'sticky',
  top: 0,
  zIndex: 1,
  // Card tint is translucent in dark mode; pinned content needs an opaque surface.
  background: theme.surface.floating,
  paddingBlock: 12,
  alignItems: 'start',
})

// Decision controls at the end of a card.
export const footer = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: '12px',
  justifyContent: 'flex-end',
  minWidth: 0,
})

export const stickyFooter = style({
  position: 'sticky',
  bottom: 0,
  zIndex: 1,
  // Pinned content needs an opaque surface, like the sticky header.
  background: theme.surface.floating,
  borderTop: `1px solid ${theme.stroke.secondary}`,
  paddingBlock: 12,
})

// Copy owns its case: interface words stay lowercase.
export const title = style({
  flexShrink: 1,
  minWidth: 0,
})

export const headerPill = styleVariants({
  end: { flexShrink: 0, marginLeft: 'auto' },
  title: { flexShrink: 0 },
})

export const description = style({
  ...utils.boxClamp(5),
})

// A card with an action: the title and description take the row, and the
// action keeps its width at the right edge, centred against them.
export const withAction = style({
  alignItems: 'center',
  display: 'flex',
  gap: '16px',
  minWidth: 0,
})

export const summary = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: '8px',
  minWidth: 0,
})

export const action = style({
  display: 'flex',
  flexShrink: 0,
  gap: '8px',
})

export const inlinePill = style({ marginLeft: 0 })
