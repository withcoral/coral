import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const statusCell = style({
  paddingBlock: '24px',
  paddingInline: '12px',
  textAlign: 'center',
})

export const tableContainer = style({
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: '10px',
  overflow: 'hidden',
})

export const table = style({
  tableLayout: 'fixed',
})

// `table-layout: fixed` takes column widths from the first row, so this must
// also go on the header cell.
export const enabledColumn = style({
  verticalAlign: 'top',
  width: '96px',
})

// Wax table cells are built for short single-line values: they clamp to 250px
// and ellipsize rather than wrap. A feature carries a sentence of prose, so this
// cell opts out of all three.
export const featureCell = style({
  maxWidth: 'none',
  overflow: 'visible',
  whiteSpace: 'normal',
})

export const feature = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '2px',
})
