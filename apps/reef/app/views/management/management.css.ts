import { createContainer, style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/breakpoints'
import { utils } from '@/styles/utils'
import { theme } from '@/wax/theme/theme.css'

export const stack = style({ display: 'flex', flexDirection: 'column', gap: 16, minWidth: 0 })
// The card shows the prompt's first three lines. Copy prompt copies all of it.
export const onboardingPrompt = style({ ...utils.boxClamp(3), margin: 0 })
export const intro = style({ display: 'flex', flexDirection: 'column', gap: 6, minWidth: 0 })
export const heading = style({
  display: 'flex',
  alignItems: 'center',
  flexWrap: 'wrap',
  gap: 12,
  overflowWrap: 'anywhere',
})
export const headerActions = style({
  display: 'flex',
  gap: 12,
  marginInlineStart: 'auto',
  flexWrap: 'wrap',
})
export const sectionHeading = style({
  alignItems: 'flex-start',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  justifyContent: 'space-between',
})
export const plainList = style({
  display: 'flex',
  flexDirection: 'column',
  listStyle: 'none',
  margin: 0,
  padding: 0,
})
// A card's few rows and the link to the rest sit close, apart from the card's other parts.
export const glimpse = style({ display: 'flex', flexDirection: 'column', gap: 4, minWidth: 0 })
export const glimpseMore = style({ alignSelf: 'flex-start' })
export const listRow = style({
  alignItems: 'center',
  borderTop: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  justifyContent: 'space-between',
  minWidth: 0,
  paddingBlock: 8,
  selectors: { '&:first-child': { borderTop: 0, paddingTop: 0 } },
})
export const section = style({ display: 'flex', flexDirection: 'column', gap: 16, minWidth: 0 })
export const panel = style({ gap: 20, minWidth: 0 })
export const quickStart = style({ gap: 16, minWidth: 0 })
// An agent's setup leads; accounts and a short Activity reader share the row below it.
// The dashboard is its own container: the stack point follows its width, not the viewport's.
const agentDashboardContainer = createContainer()
const STACKED_DASHBOARD = `${agentDashboardContainer} (max-width: 960px)`
// The cards fill the page's height and scroll their rows. Below this, the page scrolls instead.
const AGENT_CARD_MIN_HEIGHT = 360
export const agentDashboard = style({
  containerName: agentDashboardContainer,
  containerType: 'inline-size',
  display: 'flex',
  flex: '1 1 0',
  flexDirection: 'column',
  gap: 16,
  minWidth: 0,
})
export const agentDashboardGrid = style({
  display: 'grid',
  flex: '1 1 0',
  gap: 16,
  gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
  gridTemplateRows: 'minmax(0, 1fr)',
  minHeight: AGENT_CARD_MIN_HEIGHT,
  minWidth: 0,
  '@container': {
    // Activity goes below the accounts, and the page scrolls past both.
    [STACKED_DASHBOARD]: {
      flex: 'none',
      gridTemplateColumns: 'minmax(0, 1fr)',
      gridTemplateRows: 'auto',
    },
  },
})
export const agentDashboardSingle = style([
  agentDashboardGrid,
  { gridTemplateColumns: 'minmax(0, 1fr)' },
])
// A long list scrolls inside the card instead of stretching the dashboard row.
export const agentAccounts = style([section, { minHeight: 0 }])
export const agentRecentActivity = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  minHeight: 0,
  minWidth: 0,
  '@container': {
    // Stacked, the page scrolls, so the reader takes a height of its own.
    [STACKED_DASHBOARD]: { height: `clamp(${AGENT_CARD_MIN_HEIGHT}px, 70vh, 720px)` },
  },
})
export const agentRecentActivityBody = style({
  display: 'flex',
  flex: '1 1 0',
  flexDirection: 'column',
  minHeight: 0,
  minWidth: 0,
})
// Above the overview's cards, the setup spans their row.
export const overviewQuickStart = style({ gap: 16, minWidth: 0 })
// The title, with the way to replace a lost key at its far end.
export const quickStartHeader = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'baseline',
  justifyContent: 'space-between',
  gap: '4px 16px',
})
export const quickStartLost = style({ display: 'inline-flex', alignItems: 'baseline', gap: 4 })
export const quickStartTabs = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  minWidth: 0,
})
// Every client's panel stays mounted in one cell, so the setup is as tall as the
// longest one and switching tabs never moves what is below it.
export const quickStartPanels = style({ display: 'grid', minWidth: 0 })
export const quickStartPanel = style({
  gridArea: '1 / 1',
  minWidth: 0,
  selectors: { '&[hidden]': { display: 'block', visibility: 'hidden' } },
})
export const quickStartSteps = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  listStyle: 'none',
  margin: 0,
  padding: 0,
})
// An icon beside its label in a table cell: one line, so the cell truncates it.
export const cellInline = style({ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 })
export const cellInlineText = style({
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
// A status above the reasons it lists. The reasons wrap, so none is cut off.
export const cellStack = style({ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 })
// Inline, so the cell's alignment reaches a form or link inside it.
export const cellAction = style({ display: 'inline-flex' })
export const row = style({ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 12 })
export const formRow = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'flex-end',
  gap: 12,
})
export const field = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  flex: '1 1 auto',
  minWidth: 0,
  maxWidth: 400,
})
export const roleTrigger = style({ justifyContent: 'space-between', minWidth: 120 })
export const divider = style({ borderTop: `1px solid ${theme.stroke.secondary}`, paddingTop: 20 })
// Cards in one row share its height; a short list does not shrink its card.
export const cards = style({
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 260px), 1fr))',
  gap: 16,
})
// A label column beside its explanation, so short and long entries share one shape.
export const guidance = style({
  borderTop: `1px solid ${theme.stroke.secondary}`,
  display: 'grid',
  gap: '4px 24px',
  gridTemplateColumns: '96px minmax(0, 1fr)',
  paddingTop: 16,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: { gridTemplateColumns: 'minmax(0, 1fr)' },
  },
})
export const identity = style({ overflowWrap: 'anywhere', minWidth: 0 })
// A person on one table line: the avatar and any pill keep their size, the name truncates.
export const personLine = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  minWidth: 0,
})
export const personLineName = style({ minWidth: 0 })
export const empty = style({
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  gap: 8,
  padding: '32px 16px',
  textAlign: 'center',
})
export const secret = style({
  whiteSpace: 'pre-wrap',
  overflowWrap: 'anywhere',
  userSelect: 'all',
  padding: 16,
})

// One line under the breadcrumb: context on the left, page actions on the right.
export const pageIntro = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  minWidth: 0,
})

// People beside their role in the add dialog; the role stacks below on a phone.
export const addFields = style({
  display: 'grid',
  gridTemplateColumns: 'minmax(0, 1fr) 140px',
  alignItems: 'start',
  gap: 12,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: { gridTemplateColumns: 'minmax(0, 1fr)' },
  },
})
// The role menu alone, as wide as the role column in the add dialog.
export const roleField = style({ display: 'flex', flexDirection: 'column', gap: 8, width: 140 })
export const addField = style({ display: 'flex', flexDirection: 'column', gap: 8, minWidth: 0 })

// A page's name as its heading, with an editor in its place while you rename it.
export const nameHeading = style({
  alignItems: 'center',
  display: 'flex',
  gap: 4,
  minWidth: 0,
  overflowWrap: 'anywhere',
})
export const nameForm = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
})
export const nameRow = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  minWidth: 0,
})
export const nameInput = style({ flex: '1 1 240px', maxWidth: 480, minWidth: 0 })
// In the top bar the editor takes the title's place on one line: the breadcrumb
// sizes to its content, so a row free to wrap would drop its buttons under the input.
export const titleNameRow = style({ flexWrap: 'nowrap' })
export const titleNameInput = style({ flex: '0 1 280px', minWidth: 160 })

// What a confirmed action changes, as a short list between two sentences.
export const consequences = style({ display: 'flex', flexDirection: 'column', gap: 8 })
export const bulletList = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  margin: 0,
  paddingLeft: 20,
  listStyle: 'disc',
})
