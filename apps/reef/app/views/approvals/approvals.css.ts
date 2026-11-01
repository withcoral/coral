import { style } from '@vanilla-extract/css'

/** The request beside its actions, which wrap under it when the row has no room. */
export const row = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: 12,
})
export const request = style({
  display: 'flex',
  flex: '1 1 320px',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
})
export const actions = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  gap: 12,
})
