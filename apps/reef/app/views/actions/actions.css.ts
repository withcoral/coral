import { style } from '@vanilla-extract/css'

export const stack = style({ display: 'flex', flexDirection: 'column', gap: 12, minWidth: 0 })
export const summary = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
  flex: 1,
})
export const title = style({ overflowWrap: 'anywhere' })
export const controls = style({ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 12 })
export const review = style({ display: 'flex', flexDirection: 'column', gap: 16, minWidth: 0 })
export const actions = style({
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  justifyContent: 'flex-end',
})
export const technical = style({ overflowWrap: 'anywhere', whiteSpace: 'pre-wrap' })
