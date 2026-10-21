import { globalStyle, style } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'

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
export const tile = style({ gap: 4, minWidth: 0 })
export const tileLabel = style({
  display: 'flex',
  alignItems: 'center',
  gap: 6,
})
export const tileValue = style({
  fontVariantNumeric: 'tabular-nums',
})

export const bar = style({
  display: 'flex',
  height: 8,
  borderRadius: 4,
  overflow: 'hidden',
  background: theme.surface.onMainContent,
})
// Every bar shares one tone; each row's icon and name say what it counts.
export const fill = style({ minWidth: 2, background: theme.chart.blue.fill })
export const count = style({
  marginLeft: 'auto',
  fontVariantNumeric: 'tabular-nums',
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
export const rowBar = style({ gridColumn: '1 / -1', height: 4 })
export const split = style({
  display: 'flex',
  alignItems: 'center',
  gap: 10,
  fontVariantNumeric: 'tabular-nums',
})
export const splitPart = style({
  display: 'inline-flex',
  alignItems: 'center',
  gap: 4,
})

export const stats = style({
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'repeat(3, minmax(0, 1fr))',
  margin: 0,
})
export const stat = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  margin: 0,
})
export const statValue = style({ margin: 0, fontVariantNumeric: 'tabular-nums' })
