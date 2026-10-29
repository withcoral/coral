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
export const panels = style({ display: 'grid' })
export const panel = style({
  gridArea: '1 / 1',
  minWidth: 0,
  padding: 16,
  outline: 'none',
  selectors: {
    // A hidden lens still takes up its space, so it sizes the shared cell.
    '&[hidden]': { display: 'block', visibility: 'hidden' },
  },
})
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

export const tool = style({
  display: 'inline-flex',
  alignItems: 'center',
  gap: 8,
  minWidth: 0,
  maxWidth: '100%',
})
// A long provider name gives way before the tool does.
export const toolProvider = style({
  flexShrink: 1,
  minWidth: 0,
  maxWidth: '50%',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
export const barCell = style({
  display: 'grid',
  gridTemplateColumns: '48px minmax(0, 1fr)',
  alignItems: 'center',
  gap: 8,
})

export const tools = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  justifyContent: 'flex-end',
  gap: 12,
})
export const filter = style({ flex: '0 1 280px' })
export const shown = style({ fontVariantNumeric: 'tabular-nums' })
export const section = style({ display: 'flex', flexDirection: 'column', gap: 8 })
export const sectionHeading = style({ display: 'flex', alignItems: 'baseline', gap: 8 })
export const statCards = style({
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
})
