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
// The metric tabs run to the card's edges, above the panel's padding.
export const card = style({ padding: 0, overflow: 'hidden' })
export const panel = style({ padding: 16, outline: 'none' })
export const lens = style({ display: 'flex', flexDirection: 'column', gap: 16 })

export const labeled = style({
  display: 'inline-flex',
  alignItems: 'center',
  gap: 4,
  minWidth: 0,
})
export const truncate = style({
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
export const count = style({ fontVariantNumeric: 'tabular-nums' })
export const stopSplit = style({ display: 'inline-flex', alignItems: 'center', gap: 10 })

export const operation = style({
  display: 'inline-flex',
  alignItems: 'center',
  gap: 8,
  minWidth: 0,
  maxWidth: '100%',
})
export const operationProvider = style({ flexShrink: 0 })
// An operation under its provider's heading lines up after the heading's toggle and logo.
export const grouped = style({ display: 'block', minWidth: 0, paddingLeft: 58 })
export const groupName = style({
  display: 'inline-flex',
  alignItems: 'center',
  gap: 8,
  minWidth: 0,
})
export const barCell = style({
  display: 'grid',
  gridTemplateColumns: '48px minmax(0, 1fr)',
  alignItems: 'center',
  gap: 8,
})

export const tools = style({ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 12 })
export const filter = style({ flex: '0 1 280px' })
export const shown = style({ marginRight: 'auto', fontVariantNumeric: 'tabular-nums' })
export const split = style({ display: 'flex', flexDirection: 'column', gap: 10 })
export const splitBar = style({
  display: 'flex',
  gap: 2,
  height: 8,
  borderRadius: 4,
  overflow: 'hidden',
})
export const splitSegment = style({ minWidth: 3, flexBasis: 0 })
export const fillAnswered = style({ background: theme.chart.neutral.fill })
export const fillStopped = style({ background: theme.chart.amber.fill })
export const fillFailed = style({ background: theme.chart.red.fill })
export const fillCanceled = style({ background: theme.content.disabled })
export const legend = style({
  display: 'flex',
  flexWrap: 'wrap',
  gap: '6px 20px',
  margin: 0,
  padding: 0,
  listStyle: 'none',
})
export const legendItem = style({ display: 'inline-flex', alignItems: 'center', gap: 6 })
export const swatch = style({ width: 8, height: 8, borderRadius: 2, flexShrink: 0 })
export const section = style({ display: 'flex', flexDirection: 'column', gap: 8 })
export const sectionHeading = style({ display: 'flex', alignItems: 'baseline', gap: 8 })
export const stats = style({
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))',
  padding: '4px 0 8px',
})
