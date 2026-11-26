import { recipe, style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

export const journey = style({ display: 'flex', flexDirection: 'column', gap: 32, minWidth: 0 })
export const rail = style({
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))',
  gap: 16,
  listStyle: 'none',
  margin: 0,
  padding: 16,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 12,
  background: theme.surface.card,
  '@media': { '(max-width: 700px)': { gridTemplateColumns: 'minmax(0, 1fr)' } },
})
// Phones show only the current step, as "Step 2 of 4" above its name.
export const railStep = style({
  display: 'flex',
  alignItems: 'center',
  gap: 10,
  minWidth: 0,
  '@media': {
    '(max-width: 700px)': { selectors: { '&:not([aria-current])': { display: 'none' } } },
  },
})
export const railText = style({ display: 'flex', flexDirection: 'column', minWidth: 0 })
export const railCounter = style({
  display: 'none',
  '@media': { '(max-width: 700px)': { display: 'inline' } },
})
export const screen = style({ display: 'flex', flexDirection: 'column', gap: 20, minWidth: 0 })

export const list = style({
  display: 'flex',
  flexDirection: 'column',
  listStyle: 'none',
  margin: 0,
  padding: 0,
})
export const step = recipe({
  base: {
    display: 'grid',
    gap: '4px 16px',
    gridTemplateColumns: '28px minmax(0, 1fr)',
    paddingBlock: 16,
    position: 'relative',
    selectors: {
      '&:not(:last-child)::before': {
        background: theme.stroke.secondary,
        content: '""',
        insetBlock: '44px 0',
        insetInlineStart: 13,
        position: 'absolute',
        width: 2,
      },
    },
  },
  variants: {
    state: {
      done: {},
      current: {},
      upcoming: { color: theme.content.tertiary },
    },
  },
})
export const marker = recipe({
  base: {
    alignItems: 'center',
    borderRadius: '50%',
    display: 'inline-flex',
    height: 28,
    justifyContent: 'center',
    width: 28,
    ...theme.typography.bodySmallStrong,
  },
  variants: {
    state: {
      done: { background: theme.pill.green.background, color: theme.pill.green.color },
      current: {
        background: theme.button.primary.default,
        color: theme.content.accentContent.primaryReverse,
      },
      upcoming: {
        background: 'transparent',
        border: `2px solid ${theme.stroke.primary}`,
        color: theme.content.tertiary,
      },
    },
  },
})
export const heading = style({
  alignItems: 'baseline',
  display: 'flex',
  flexWrap: 'wrap',
  gap: '4px 12px',
  minHeight: 28,
  minWidth: 0,
})
export const summary = style({ minWidth: 0, overflowWrap: 'anywhere' })
export const content = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  gridColumn: 2,
  minWidth: 0,
  paddingTop: 8,
})
