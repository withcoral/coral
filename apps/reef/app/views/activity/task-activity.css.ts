import { style } from '@vanilla-extract/css'

/** The list, and the detail rail docked beside it on a wide screen, which keeps its own gap. */
export const root = style({
  display: 'flex',
  height: '100%',
  minHeight: 0,
  minWidth: 0,
  flex: 1,
})
/** Everything but the docked rail: it shrinks when the rail docks. */
export const main = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  flex: '1 1 0',
  minHeight: 0,
  minWidth: 0,
})
export const toolbar = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  gap: 12,
  selectors: { '&:empty': { display: 'none' } },
})
/** The list's heading, with the page's live toggle right after it. */
export const listHeading = style({
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  minWidth: 0,
  selectors: { '&:empty': { display: 'none' } },
})
export const filters = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  gap: 8,
  selectors: { '&:empty': { display: 'none' } },
})
export const stack = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  minWidth: 0,
})
export const list = style({
  display: 'flex',
  flexDirection: 'column',
  flex: '1 1 auto',
  minHeight: 0,
  minWidth: 0,
  gap: 16,
})
/** Sized by its rows; the history list below takes the remaining height. */
export const pending = style({
  display: 'flex',
  flexDirection: 'column',
  flexShrink: 0,
  minWidth: 0,
  gap: 12,
})
export const metadata = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
})
export const native = style({
  whiteSpace: 'pre-wrap',
  overflowWrap: 'anywhere',
  flexShrink: 0,
})

// A panel's own action, after its evidence, at the panel's end edge.
// Apart like a card's decision controls, and wrapped on a narrow screen rather than overflowing.
export const endActions = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  justifyContent: 'flex-end',
})

/** A card that is a link: it reads as the card, not as underlined link text. */
export const cardLink = style({ color: 'inherit', textDecoration: 'none' })
