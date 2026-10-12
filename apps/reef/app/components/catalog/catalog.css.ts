import { globalStyle, style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const layout = style({
  display: 'grid',
  gridTemplateColumns: '260px minmax(0, 1fr)',
  height: '100%',
  columnGap: 0,
  minHeight: 0,
  minWidth: 0,
  width: '100%',
  '@media': { '(max-width: 760px)': { gridTemplateColumns: '1fr' } },
})
export const rail = style({
  borderInlineEnd: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  minHeight: 0,
  minWidth: 0,
  paddingInlineEnd: 16,
  '@media': {
    '(max-width: 760px)': {
      borderBlockEnd: `1px solid ${theme.stroke.secondary}`,
      borderInlineEnd: 0,
      paddingBlockEnd: 16,
      paddingInlineEnd: 0,
    },
  },
})
export const filterField = style({ display: 'flex', flexDirection: 'column', minWidth: 0 })
// The rail owns its own scroll port; the page ancestor clips overflow.
export const railScroll = style({ flex: '1 1 auto', minHeight: 0 })
export const railList = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
})
// One entry in the rail: the link that scrolls the table to it, and any
// trailing control, which a caller can reveal on hover, focus, or current.
export const railItem = style({
  alignItems: 'center',
  borderRadius: 8,
  display: 'flex',
  gap: 2,
  paddingInlineEnd: 4,
  selectors: {
    '&:hover': { background: theme.sidebar.button.hover },
    '&[data-current="page"]': { background: theme.surface.onMainContentSubtle },
  },
})
export const railLink = style({
  borderRadius: 8,
  flex: '1 1 auto',
  gap: 10,
  justifyContent: 'flex-start',
  minWidth: 0,
  paddingBlock: 6,
  paddingInline: 8,
  // The entry, not the link, shows hover and focus, so both cover the
  // trailing controls too.
  selectors: {
    [`${railItem} &:hover`]: { background: 'transparent' },
    [`${railItem} &:focus-visible`]: { outline: 'none' },
  },
})
// Drawn inside the entry so the rail's scroll port does not clip it.
globalStyle(`${railItem}:has(> ${railLink}:focus-visible)`, {
  outline: `1px solid ${theme.button.primary.focus}`,
  outlineOffset: -1,
})
export const railName = style({
  flex: '1 1 auto',
  minWidth: 0,
  overflow: 'hidden',
  textAlign: 'start',
  textOverflow: 'ellipsis',
})
export const railCount = style({
  alignItems: 'center',
  background: theme.surface.onMainContentSubtle,
  borderRadius: 999,
  color: theme.content.secondary,
  display: 'inline-flex',
  flexShrink: 0,
  justifyContent: 'center',
  marginInlineStart: 'auto',
  minWidth: 24,
  paddingBlock: 2,
  paddingInline: 7,
  ...theme.typography.bodySmallStrong,
})
export const main = style({
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  minHeight: 0,
  minWidth: 0,
  paddingInlineStart: 16,
})
export const stack = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: 16,
  minHeight: 0,
  minWidth: 0,
})
export const toolbar = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  justifyContent: 'space-between',
  minWidth: 0,
})
export const toolbarGroup = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  minWidth: 0,
  // The trailing group keeps the filter at the end when the row wraps.
  selectors: { '&:last-child': { flex: '1 1 auto', justifyContent: 'flex-end' } },
})
export const toolbarFilter = style({ flex: '0 1 360px', minWidth: 220 })
