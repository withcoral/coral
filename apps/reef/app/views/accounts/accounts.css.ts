import { style } from '@vanilla-extract/css'

export const stack = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '1rem',
  alignItems: 'flex-start',
})
export const actions = style({
  display: 'flex',
  flexWrap: 'wrap',
  gap: '0.5rem',
  alignItems: 'center',
})
export const account = style({
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'flex-start',
  gap: '0.75rem',
  paddingBlock: '1rem',
})
export const reference = style({ overflowWrap: 'anywhere' })
