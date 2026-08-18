import { style } from '@vanilla-extract/css'

export const statusCell = style({
  paddingBlock: '24px',
  paddingInline: '12px',
  textAlign: 'center',
})

// The feature cell beside it wraps onto several lines. The switch stays beside
// the feature name at the top of the row rather than centred against the prose.
export const enabledCell = style({
  alignItems: 'flex-start',
  paddingBlock: '12px',
})

export const feature = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '2px',
})
