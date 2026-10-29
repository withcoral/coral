import { globalStyle, style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const stack = style({ display: 'flex', flexDirection: 'column', gap: 16, minWidth: 0 })
export const empty = style({ display: 'flex', height: '100%' })
export const table = style({ minHeight: 0 })
// The policy column ends a step in from the table's edge.
globalStyle(`${table} [role="row"] > :last-child`, { paddingInlineEnd: 24 })
export const policy = style({ display: 'inline-flex' })
export const nameHeader = style({ alignItems: 'center', display: 'flex', gap: 8 })
// The column is as wide as its toggles; the header centres over them.
export const policyHeader = style({ display: 'flex', justifyContent: 'center' })
export const policyCell = style({ alignItems: 'center', display: 'flex' })
// A namespace heading: its toggle, name and count.
export const groupLabel = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  gridColumn: '1 / -1',
  minWidth: 0,
})
// The name lines up with the column heading's text, after the toggle's room.
export const nameCell = style({
  alignItems: 'center',
  display: 'flex',
  minWidth: 0,
  paddingInlineStart: 42,
})
export const name = style({
  display: 'block',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  ...theme.typography.bodySmall,
})
export const callPath = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  minWidth: 0,
})
