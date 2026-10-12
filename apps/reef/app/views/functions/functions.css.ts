import { style } from '@vanilla-extract/css'

export const stack = style({ display: 'flex', flexDirection: 'column', gap: 16, minWidth: 0 })
export const empty = style({ display: 'flex', height: '100%' })
export const table = style({ minHeight: 0 })
export const policy = style({ display: 'inline-flex' })
export const name = style({ display: 'flex', minWidth: 0 })
// Wax insets a group's label one cell padding (12px) past the column; its
// members match it so the label heads them.
export const groupedName = style([name, { paddingInlineStart: 12 }])
export const callPath = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  minWidth: 0,
})
