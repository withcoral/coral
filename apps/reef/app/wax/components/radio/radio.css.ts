import { style } from '@/wax/css'

import { animation, theme } from '@/wax/theme/theme.css'

export const group = style({
  alignItems: 'center',
  display: 'flex',
  gap: 20,
  minWidth: 'max-content',
  paddingBlock: 4,
  paddingInline: 4,
})

export const stack = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
  padding: 4,
})

export const grid = style({
  display: 'grid',
  gap: 8,
  gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
  minWidth: 0,
  padding: 4,
  '@media': { '(max-width: 640px)': { gridTemplateColumns: 'minmax(0, 1fr)' } },
})

export const item = style({
  alignItems: 'center',
  color: theme.content.secondary,
  cursor: 'pointer',
  display: 'inline-flex',
  flexShrink: 0,
  gap: 8,
  userSelect: 'none',
  whiteSpace: 'nowrap',
  ...theme.typography.body,
  selectors: {
    [`${stack} &`]: { whiteSpace: 'normal', overflowWrap: 'anywhere', minWidth: 0 },
    '&:hover:not(:has([data-disabled]))': {
      color: theme.content.primary,
    },
    '&:has([data-checked])': {
      color: theme.content.primary,
    },
    '&:has([data-disabled])': {
      color: theme.content.disabled,
      cursor: 'not-allowed',
    },
    '&:has([role="radio"]:focus-visible)': {
      borderRadius: 4,
      outline: `2px solid ${theme.stroke.focused}`,
      outlineOffset: 2,
    },
  },
})

export const control = style({
  alignItems: 'center',
  backgroundColor: 'transparent',
  border: `2px solid ${theme.content.tertiary}`,
  borderRadius: '50%',
  cursor: 'inherit',
  display: 'inline-flex',
  flexShrink: 0,
  height: 18,
  justifyContent: 'center',
  outline: 'none',
  padding: 0,
  transition: animation.colorTransition,
  width: 18,
  selectors: {
    '&[data-checked]': {
      borderColor: theme.stroke.focused,
    },
    '&[data-disabled]': {
      borderColor: theme.input.stroke.disabled,
    },
  },
})

export const indicator = style({
  backgroundColor: theme.stroke.focused,
  borderRadius: '50%',
  height: 8,
  width: 8,
})

export const label = style({
  pointerEvents: 'none',
})

export const copy = style({ display: 'flex', flexDirection: 'column', gap: 4, minWidth: 0 })

export const card = style({
  alignItems: 'flex-start',
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 10,
  gap: 12,
  padding: 16,
  whiteSpace: 'normal',
  overflowWrap: 'anywhere',
  selectors: {
    '&:has([data-checked])': { borderColor: theme.stroke.focused },
    '&:hover:not(:has([data-disabled]))': { background: theme.surface.onMainContentHover },
    '&:has([data-disabled])': { opacity: 0.6 },
  },
})

// A card with controls: actions wrap below the copy rather than squeezing its text.
export const cardStack = style({ flexDirection: 'column' })
export const cardRow = style({
  alignItems: 'flex-start',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  width: '100%',
})
export const cardLabel = style({
  alignItems: 'flex-start',
  cursor: 'pointer',
  display: 'flex',
  flex: '1 1 220px',
  gap: 12,
  minWidth: 0,
})
export const cardAside = style({ flex: '0 0 auto', marginInlineStart: 'auto' })
export const cardAction = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
  paddingInlineStart: 30,
  width: '100%',
})
