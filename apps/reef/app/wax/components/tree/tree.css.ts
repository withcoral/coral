import { globalStyle, recipe, style } from '@/wax/css'

import { animation, theme } from '@/wax/theme/theme.css'

export const tree = style({
  listStyle: 'none',
  margin: 0,
  minWidth: 0,
  padding: 0,
})
export const group = style({
  listStyle: 'none',
  margin: 0,
  padding: 0,
})
export const item = style({
  minWidth: 0,
  outline: 'none',
})
// The focus ring sits on the row, not the whole subtree the item contains.
globalStyle(`${item}:focus-visible > [data-row]`, {
  outline: `2px solid ${theme.stroke.focused}`,
  outlineOffset: -2,
})
export const row = recipe({
  base: {
    alignItems: 'center',
    borderRadius: 6,
    color: theme.content.primary,
    display: 'flex',
    gap: 6,
    minHeight: 30,
    minWidth: 0,
    paddingInlineEnd: 8,
    transition: animation.colorTransition,
    selectors: {
      '&:hover': { background: theme.surface.onMainContentHover },
    },
  },
  variants: {
    dimmed: {
      true: { color: theme.content.tertiary },
      false: {},
    },
  },
})
export const toggle = style({
  alignItems: 'center',
  background: 'transparent',
  border: 0,
  borderRadius: 4,
  color: theme.content.tertiary,
  cursor: 'pointer',
  display: 'inline-flex',
  flexShrink: 0,
  height: 22,
  justifyContent: 'center',
  padding: 0,
  width: 22,
  selectors: { '&:hover': { color: theme.content.primary } },
})
export const marker = recipe({
  base: {
    borderRadius: '50%',
    display: 'inline-block',
    flexShrink: 0,
    height: 8,
    marginInline: 7,
    width: 8,
  },
  variants: {
    kind: {
      solid: { background: theme.content.tertiary },
      hollow: { border: `1.5px solid ${theme.content.tertiary}` },
      none: { visibility: 'hidden' },
    },
  },
})
export const label = style({
  background: 'transparent',
  border: 0,
  color: 'inherit',
  cursor: 'pointer',
  flex: '1 1 auto',
  minWidth: 0,
  overflow: 'hidden',
  padding: '4px 0',
  textAlign: 'start',
  textDecoration: 'none',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  ...theme.typography.body,
  selectors: { '&:focus': { outline: 'none' } },
})
// A count is a few characters that always fit: it needs no room to spare, and
// giving it any would let it vanish before the label it qualifies.
export const detail = style({
  color: theme.content.tertiary,
  flexShrink: 0,
  whiteSpace: 'nowrap',
  ...theme.typography.bodySmall,
})
// A summary is a column of its own on the right of the row, so summaries start
// at the same place down the list rather than wherever each name happens to
// end. It never grows into the name's half, and what does not fit is clipped
// here rather than widening the row.
export const summary = style({
  color: theme.content.tertiary,
  flex: '0 1 45%',
  minWidth: 0,
  overflow: 'hidden',
  paddingInlineStart: 32,
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  ...theme.typography.bodySmall,
})
export const trailing = style({ display: 'inline-flex', flexShrink: 0, marginInlineStart: 'auto' })
export const empty = style({
  color: theme.content.tertiary,
  padding: '12px 8px',
  ...theme.typography.bodySmall,
})
