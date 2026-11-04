import { style } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'

/** The list, and the detail rail docked beside it on a wide screen, which keeps its own gap. */
export const root = style({
  display: 'flex',
  height: '100%',
  minHeight: 0,
  minWidth: 0,
  flex: 1,
})
/** The list of Tasks and standalone Runs, beside the open one's timeline. */
export const rootList = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  width: 300,
  flex: 'none',
  minHeight: 0,
  paddingRight: 12,
  borderRight: `1px solid ${theme.stroke.secondary}`,
})
/** The open root's timeline: it takes the width the list and the docked rail leave. */
export const timeline = style({
  display: 'flex',
  flex: '1 1 0',
  minHeight: 0,
  minWidth: 0,
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

/** A root's start and its chips, on the line under its name. */
export const rootFacts = style({ display: 'inline-flex', alignItems: 'center', gap: 6 })

/** The agent's own summary of a Task, leading its panel. */
export const agentSummary = style({ display: 'flex', flexDirection: 'column', gap: 4 })

/** A call's total, running and waiting time, side by side. */
export const callFigures = style({ display: 'flex', gap: 24 })
export const callFigure = style({ display: 'flex', flexDirection: 'column' })
/** What a call returned, under its heading. */
export const callResponse = style({ display: 'flex', flexDirection: 'column', gap: 8 })
