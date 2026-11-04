import { style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

export const waterfall = style({
  flex: 1,
  display: 'flex',
  flexDirection: 'column',
  minWidth: 0,
  minHeight: 0,
  height: '100%',
})

export const header = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  padding: '12px 16px',
  borderBottom: `1px solid ${theme.stroke.secondary}`,
})

export const title = style({ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 })

export const facts = style({
  display: 'flex',
  alignItems: 'center',
  flexWrap: 'wrap',
  gap: '4px 12px',
})

export const fact = style({ display: 'inline-flex', alignItems: 'center', gap: 4 })

/** The root's facts, with where its time went beside them. */
export const summary = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'flex-start',
  justifyContent: 'space-between',
  gap: '8px 24px',
})

/** Where the root's time went: a short share beside the facts. */
export const minimap = style({
  padding: '8px 16px',
  borderBottom: `1px solid ${theme.stroke.secondary}`,
})

export const toolbar = style({
  display: 'flex',
  alignItems: 'center',
  flexWrap: 'wrap',
  gap: '8px 12px',
  padding: '8px 16px',
  borderBottom: `1px solid ${theme.stroke.secondary}`,
})

export const rows = style({ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' })

/**
 * The timeline cell's box: bars and marks are placed across it. A cell wraps its content in
 * a box with no height of its own, so the track states one.
 */
export const track = style({ position: 'relative', height: 24, width: '100%' })

export const axisHeading = style({ position: 'relative', height: 20, width: '100%' })

/** The root's name in the header: one line, cut when it does not fit. */
export const label = style({
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

/** The failure's error, then each fact about it, one per line. */
export const failure = style({ display: 'flex', flexDirection: 'column', gap: 4 })

/** The failure's error as Coral recorded it, word for word. */
export const error = style({
  display: 'block',
  whiteSpace: 'pre-wrap',
  overflowWrap: 'anywhere',
  fontFamily: theme.typography.code.fontFamily,
  fontSize: theme.typography.codeSmallInline.fontSize,
  color: theme.content.primary,
})

/** The failure's actions, right under its text. */
export const failureActions = style({
  display: 'inline-flex',
  alignItems: 'center',
  gap: 8,
  marginTop: 4,
})

/** A record's context bytes, over a bar in proportion to the most any record added. */
export const context = style({
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'flex-end',
  justifyContent: 'center',
  gap: 2,
  height: 28,
})
export const contextBar = style({
  height: 3,
  borderRadius: 2,
  background: theme.chart.neutral.fill,
})
export const contextValue = style({ whiteSpace: 'nowrap' })

/** A bar's breakdown on hover. */
export const barCard = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  paddingBlock: 4,
  minWidth: 240,
})

/** The agent's own summary under the title: its line breaks kept, the most read text. */
export const agentSummary = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  paddingInlineStart: 10,
  borderInlineStart: `2px solid ${theme.stroke.secondary}`,
})
export const agentText = style({ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' })

/** The run picker, pushed to the end of the title row. */
export const picker = style({ marginInlineStart: 'auto' })

/** The root's name: up to two lines, then cut. */
export const heading = style({
  minWidth: 0,
  overflow: 'hidden',
  display: '-webkit-box',
  WebkitLineClamp: 2,
  WebkitBoxOrient: 'vertical',
})
/** The root's facts, one strip that wraps between facts. */
export const stats = style({ fontVariantNumeric: 'tabular-nums' })

/** The zoom control: out, the factor, in. */
export const zoom = style({ display: 'inline-flex', alignItems: 'center', gap: 4 })
export const zoomValue = style({
  minWidth: 40,
  textAlign: 'center',
  fontVariantNumeric: 'tabular-nums',
})
/** A reported issue's one line: its toggle, then what it is about. */
export const issueTitle = style({ display: 'inline-flex', alignItems: 'center', gap: 4 })

/** The key under the rows: each shape the timeline draws, and what it means. */
export const legend = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'center',
  gap: '4px 16px',
  padding: '8px 16px',
  borderTop: `1px solid ${theme.stroke.secondary}`,
})
export const legendItem = style({ display: 'inline-flex', alignItems: 'center', gap: 6 })
export const legendBracket = style({
  width: 18,
  height: 5,
  border: `1.5px solid ${theme.chart.blue.fill}`,
  borderBottom: 'none',
  borderRadius: '2px 2px 0 0',
})
export const legendDots = style({
  width: 18,
  height: 3,
  background: `radial-gradient(circle, ${theme.content.tertiary} 1px, transparent 1.5px) 0 50% / 5px 5px repeat-x`,
})
