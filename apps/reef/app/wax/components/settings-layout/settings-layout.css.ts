import { globalStyle, style } from '@/wax/css'

import { breakpoints } from '@/styles/breakpoints'
import { theme } from '@/wax/theme/theme.css'

// One resource's settings: the header over a column of cards, with an optional
// aside beside them, and removal closing the page apart from the rest.
export const page = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 48,
  // The two columns and the gap between them; removal's rule spans the same.
  maxWidth: 848,
  // Centred in a wide page, so the cards and their aside read as one column.
  marginInline: 'auto',
  minWidth: 0,
  width: '100%',
  '@media': { [`screen and (max-width: ${breakpoints.mobile})`]: { gap: 32 } },
})
// The header heads both columns, so the first card and the aside start on the
// same line.
export const top = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  minWidth: 0,
})
export const header = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  minWidth: 0,
})
// The cards' column with the aside beside it; narrower screens stack them.
export const columns = style({
  alignItems: 'start',
  display: 'grid',
  columnGap: 48,
  gridTemplateColumns: 'minmax(0, 480px) minmax(0, 320px)',
  rowGap: 32,
  '@media': { '(max-width: 1040px)': { gridTemplateColumns: 'minmax(0, 480px)' } },
})
// A wide list beside the same aside: the main column takes the free width.
export const wideColumns = style({
  alignItems: 'start',
  display: 'grid',
  columnGap: 32,
  gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 320px)',
  minWidth: 0,
  rowGap: 24,
  '@media': { '(max-width: 1040px)': { gridTemplateColumns: 'minmax(0, 1fr)' } },
})
export const wideMain = style({ minWidth: 0 })
export const main = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 32,
  gridColumn: 1,
  minWidth: 0,
})
// Each card in the main column fills it.
globalStyle(`${main} > *`, { minWidth: 0, width: '100%' })

// The aside: a card of facts, each its label over its value.
export const aside = style({ gap: 16, minWidth: 0 })
export const facts = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  margin: 0,
  minWidth: 0,
})
export const fact = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
})
export const factValue = style({ margin: 0, minWidth: 0 })

// Removal: its heading and consequence take the row, the action keeps the right edge.
export const removal = style({
  alignItems: 'center',
  borderTop: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexWrap: 'wrap',
  gap: 16,
  justifyContent: 'space-between',
  paddingTop: 24,
})
export const removalText = style({
  display: 'flex',
  flex: '1 1 240px',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
})
