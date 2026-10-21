import { globalStyle, style } from '@vanilla-extract/css'

export const root = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  minWidth: 0,
})
// Numbers that still cover an earlier period fade until the chosen period's arrive.
export const pending = style({})
globalStyle(`${pending} > :not(:first-child)`, { opacity: 0.5 })
export const heading = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: 12,
})
export const strip = style({
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
})
export const panels = style({
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'repeat(auto-fit, minmax(min(320px, 100%), 1fr))',
})

export const rows = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 10,
  margin: 0,
  padding: 0,
  listStyle: 'none',
})
export const row = style({
  display: 'grid',
  gridTemplateColumns: 'minmax(0, 1fr) auto',
  alignItems: 'center',
  columnGap: 12,
  rowGap: 4,
})
export const rowName = style({
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
export const count = style({
  marginLeft: 'auto',
  fontVariantNumeric: 'tabular-nums',
})
export const rowMeter = style({ gridColumn: '1 / -1' })
export const split = style({
  display: 'flex',
  alignItems: 'center',
  gap: 10,
})
export const labeled = style({
  display: 'inline-flex',
  alignItems: 'center',
  gap: 4,
  minWidth: 0,
})

export const stats = style({
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'repeat(3, minmax(0, 1fr))',
})
