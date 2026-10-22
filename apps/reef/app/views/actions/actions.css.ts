import { style } from '@vanilla-extract/css'

export const stack = style({ display: 'flex', flexDirection: 'column', gap: 12, minWidth: 0 })
export const controls = style({ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 12 })
export const review = style({ display: 'flex', flexDirection: 'column', gap: 16, minWidth: 0 })
/** One action card: what it is beside its review, then its kind and deadline. */
export const action = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: 12,
  alignSelf: 'stretch',
  minWidth: 0,
})
export const actionMeta = style({ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8 })
/** A map key beside what the call does to it. */
export const entryKey = style({ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 })
