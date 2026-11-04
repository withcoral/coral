import { style } from '@vanilla-extract/css'

/** Several lines in one value of a description list. */
export const stack = style({ display: 'flex', flexDirection: 'column', gap: 4, minWidth: 0 })

/** A name and its figure on one line. */
export const line = style({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: 8,
  minWidth: 0,
})
