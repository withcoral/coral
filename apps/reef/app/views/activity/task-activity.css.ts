import { createVar, fallbackVar, style } from '@vanilla-extract/css'
import { narrow } from './waterfall.css'

/** How wide the waterfall's context column is. The page narrows it to nothing beside a docked rail. */
const contextWidth = createVar()
/** The context column's width, as a table column takes it. */
export const contextColumn = fallbackVar(contextWidth, '88px')

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
/** The list of top-level records and the open one's waterfall, side by side. */
export const panes = style({
  display: 'flex',
  flex: '1 1 0',
  minHeight: 0,
  minWidth: 0,
})
/** The list of top-level records, beside the waterfall of the open one. */
export const rootPane = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  flex: '0 0 clamp(300px, 30%, 440px)',
  minHeight: 0,
  minWidth: 0,
  '@media': {
    [narrow]: {
      flex: '1 1 0',
      selectors: { '[data-activity-view="trace"] &': { display: 'none' } },
    },
  },
  // A docked rail takes its share of the page's row, which is then a size container.
  // Unless that row is very wide, the list narrows to its times and names meanwhile, so
  // arrivals still show and the waterfall keeps most of the room.
  '@container': {
    '(max-width: 1600px)': {
      selectors: {
        '[data-activity-rail="open"][data-activity-view] &': { flex: '0 0 220px' },
      },
    },
  },
})
/** The open record's waterfall. A narrow screen shows it in place of the list. */
export const tracePane = style({
  display: 'flex',
  flexDirection: 'column',
  flex: '1 1 0',
  minHeight: 0,
  minWidth: 0,
  marginInlineStart: 16,
  '@media': {
    [narrow]: {
      marginInlineStart: 0,
      selectors: { '[data-activity-view="list"] &': { display: 'none' } },
    },
  },
  // Squeezed between the list and a docked rail, the waterfall drops its context column:
  // the rail beside it shows the selected record's context.
  '@container': {
    '(max-width: 1600px)': {
      selectors: {
        '[data-activity-rail="open"][data-activity-view] &': {
          vars: { [contextWidth]: '0px' },
        },
      },
    },
  },
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
