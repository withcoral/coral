import { style } from '@/wax/css'

import { breakpoints } from '@/styles/breakpoints'
import { theme } from '@/wax/theme/theme.css'

export const section = style({ display: 'flex', flexDirection: 'column', minWidth: 0 })
export const title = style({ paddingBottom: 8 })
export const list = style({ display: 'flex', flexDirection: 'column', margin: 0, minWidth: 0 })
// The labels take one column as wide as the widest, and the values the rest.
export const compactList = style({
  columnGap: 24,
  display: 'grid',
  gridTemplateColumns: 'max-content minmax(0, 1fr)',
  rowGap: 4,
})
export const row = style({
  alignItems: 'start',
  display: 'grid',
  gap: '4px 16px',
  gridTemplateColumns: 'minmax(96px, 1fr) minmax(0, 3fr)',
  paddingBlock: 8,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: { gridTemplateColumns: 'minmax(0, 1fr)' },
  },
  selectors: {
    // The rows share the list's columns and gaps. A row is as tall as a control, so text and
    // pickers sit evenly. Compact rows stay side by side on mobile: their labels are short.
    [`${compactList} > &`]: {
      alignItems: 'center',
      columnGap: 'normal',
      gridColumn: '1 / -1',
      gridTemplateColumns: 'subgrid',
      minHeight: 32,
      paddingBlock: 0,
    },
  },
})
export const label = style({ overflowWrap: 'anywhere' })
export const value = style({
  margin: 0,
  minWidth: 0,
  overflowWrap: 'anywhere',
  whiteSpace: 'pre-wrap',
})
// A total closes the sum above it, as on a receipt.
export const total = style({
  borderTop: `1px solid ${theme.stroke.secondary}`,
  marginTop: 4,
})
