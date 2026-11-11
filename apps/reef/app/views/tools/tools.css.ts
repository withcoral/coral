import { globalStyle, style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const stack = style({ display: 'flex', flexDirection: 'column', gap: 16, minWidth: 0 })
// The table and its drawer, side by side, under the toolbar: docked, the drawer takes
// its share of the row.
export const dockRow = style({ display: 'flex', flex: '1 1 auto', minHeight: 0, minWidth: 0 })
// The table: it shrinks when the drawer docks.
export const dockMain = style({
  display: 'flex',
  flex: '1 1 0',
  flexDirection: 'column',
  minHeight: 0,
  minWidth: 0,
})
export const callPath = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  minWidth: 0,
})
export const pills = style({ alignItems: 'center', display: 'flex', flexWrap: 'wrap', gap: 8 })
export const emptyProviders = style({ display: 'flex', height: '100%' })
// The table scrolls, so the browser can focus it. While a keyboard cursor row
// shows where the keys act, the table draws no focus outline of its own.
export const toolsTable = style({
  minHeight: 0,
  selectors: { '&:focus:has([data-active])': { outline: 'none' } },
})
// The policy column ends a step in from the table's edge.
globalStyle(`${toolsTable} [role="row"] > :last-child`, { paddingInlineEnd: 24 })
// A no-match status runs its copy and its button on one centred line.
export const noMatchStatus = style({
  alignItems: 'center',
  columnGap: 6,
  display: 'inline-flex',
  flexWrap: 'wrap',
  justifyContent: 'center',
  rowGap: 4,
})
// The form lays out its controls in the Wax action bar that frames it.
export const bulkPolicyForm = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
})
export const toolGroupLabel = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  gridColumn: '2 / -1',
})
export const providerGroupLabel = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  gridColumn: '2 / -1',
  minWidth: 0,
})
// A group's name opens its page. It reads as the heading it is, and shows it
// is a link when the pointer or the keyboard reaches it.
export const groupLink = style({
  color: 'inherit',
  minWidth: 0,
  textDecoration: 'none',
  selectors: {
    '&:hover': { textDecoration: 'underline' },
    '&:focus-visible': { borderRadius: 4, outline: `2px solid ${theme.stroke.focused}` },
  },
})
// A checked row is filled, so the selection reads down the table at a glance.
export const checkedRow = style({
  backgroundColor: theme.surface.onMainContent,
  selectors: { '&:hover': { backgroundColor: theme.surface.onMainContentHover } },
})
export const toolNameCell = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  minWidth: 0,
  // Sets the name one step in from its group's label.
  paddingInlineStart: 40,
})
// The name reads as plain text; the whole row is the click target.
/** The tool's name gives way to its hints and status in a narrow cell. */
export const toolNameFace = style({ minWidth: 0 })
// The link truncates, so its tooltip can tell when the name is cut short. `ToolName`
// gives it the face every tool name shares.
export const toolName = style({
  color: 'inherit',
  display: 'block',
  minWidth: 0,
  overflow: 'hidden',
  textDecoration: 'none',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  selectors: {
    '&:focus-visible': { borderRadius: 4, outline: `2px solid ${theme.stroke.focused}` },
  },
})
export const inlineTooltipTarget = style({ display: 'inline-flex' })
// The collapse-all toggle starts the column, above the headings' own toggles.
export const nameHeader = style({ alignItems: 'center', display: 'flex', gap: 8 })
// The bars keep clear of the policy toggles beside them; the heading's label
// and its info tip sit on one line, as the header's own text does.
export const callsColumn = style({ marginInlineEnd: 16 })
// A hover anywhere in a 24px band shows the count, not only on the thin bar.
export const callsBar = style({
  display: 'flex',
  flexDirection: 'column',
  height: 24,
  justifyContent: 'center',
  width: '100%',
})
// The usage tooltip leads with the hovered day, then the window's total.
export const usageTipDay = style({ display: 'block', ...theme.typography.bodySmallStrong })
export const usageTipTotal = style({ color: theme.content.secondary, display: 'block' })
export const callsHeading = style({ alignItems: 'center', display: 'inline-flex', gap: 6 })
// The column is as wide as its toggles; the header centres over them.
export const policyHeader = style({ display: 'flex', justifyContent: 'center' })
// One line: a wrapped outcome pill would change the row's height after a save.
export const policyCell = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'nowrap',
  gap: 6,
})
// The buttons that add a catalog service from the providers rail, one per source.
export const railSources = style({ display: 'grid', gap: 8, minWidth: 0 })
// The drawer's usage: the total, the days as bars, then who called the tool.
export const usage = style({ display: 'grid', gap: 12 })
// A drawer section whose control keeps its own width under its heading.
export const drawerSection = style({ display: 'grid', gap: 12, justifyItems: 'start' })
export const usageChart = style({ display: 'grid', gap: 4 })
export const usageAxis = style({
  ...theme.typography.bodySmall,
  color: theme.content.tertiary,
  display: 'flex',
  justifyContent: 'space-between',
})
export const usageCallers = style({
  display: 'grid',
  gap: 4,
  listStyle: 'none',
  margin: 0,
  padding: 0,
})
export const usageCaller = style({
  ...theme.typography.bodySmall,
  display: 'flex',
  gap: 8,
  justifyContent: 'space-between',
  minWidth: 0,
})
// A long caller name wraps rather than push its count out of the drawer.
export const usageCallerName = style({ minWidth: 0, overflowWrap: 'anywhere' })
export const usageCallerCalls = style({ flexShrink: 0 })
// A ranked row's provider, before its tool, where no heading names it.
export const rankedProvider = style({
  alignItems: 'center',
  display: 'inline-flex',
  flexShrink: 0,
  gap: 6,
})
// The drawer's header: the group and the name, then the tool's actions and the close.
export const drawerHeader = style({
  alignItems: 'flex-start',
  display: 'flex',
  gap: 12,
  justifyContent: 'space-between',
  minWidth: 0,
})
export const drawerName = style({ display: 'grid', gap: 4, minWidth: 0 })
export const drawerGroup = style({ alignItems: 'center', display: 'flex', gap: 6, minWidth: 0 })
export const drawerHeaderActions = style({
  alignItems: 'center',
  display: 'flex',
  flexShrink: 0,
  gap: 4,
})
// The call path, then the line on where the tool comes from.
export const identity = style({ display: 'grid', gap: 6, minWidth: 0 })
// Rows of two lines, such as revisions and diagnostics, which fit the narrow drawer
// where a table's columns would cut off. A row's action sits at its end.
export const lines = style({ display: 'grid', listStyle: 'none', margin: 0, padding: 0 })
export const line = style({
  alignItems: 'center',
  display: 'flex',
  gap: 12,
  justifyContent: 'space-between',
  paddingBlock: 10,
  selectors: { '& + &': { borderTop: `1px solid ${theme.stroke.secondary}` } },
})
export const lineMain = style({ display: 'grid', gap: 2, minWidth: 0 })
// The publish dialog's path: its label, its field, and what the review found there.
export const publishPath = style({ display: 'grid', gap: 6, minWidth: 0 })
// One fact with the icon that tells its kind apart, such as what the publish replaces.
export const reviewFact = style({
  alignItems: 'start',
  display: 'flex',
  gap: 8,
  minWidth: 0,
  overflowWrap: 'anywhere',
})
// The icon keeps to the first line of text that wraps beside it.
export const reviewFactIcon = style({ flexShrink: 0, marginBlockStart: 1 })
// The publish dialog: what the owner decides on the left, the code they review on the
// right. On a narrow screen the code follows the decisions.
export const publishSplit = style({
  display: 'grid',
  gap: 24,
  gridTemplateColumns: '300px minmax(0, 1fr)',
  minWidth: 0,
  '@media': { '(max-width: 760px)': { gridTemplateColumns: 'minmax(0, 1fr)' } },
})
export const publishSide = style({
  alignContent: 'start',
  borderInlineEnd: `1px solid ${theme.stroke.secondary}`,
  display: 'grid',
  gap: 20,
  minWidth: 0,
  paddingInlineEnd: 24,
  '@media': { '(max-width: 760px)': { borderInlineEnd: 'none', paddingInlineEnd: 0 } },
})
// One labelled part of the left pane: its label, then what it holds.
export const publishPart = style({ display: 'grid', gap: 8, justifyItems: 'start', minWidth: 0 })
// The publish dialog's footer stays at the popup's bottom edge, so the policy
// and the publish stay in view however far the owner scrolls. It reaches
// through the popup's padding to its edges, which is 24px on each side.
export const publishFooter = style({
  background: theme.surface.floating,
  borderTop: `1px solid ${theme.stroke.secondary}`,
  display: 'grid',
  gap: 12,
  marginBlockEnd: -24,
  paddingBlock: '12px 24px',
  paddingInline: 24,
  position: 'sticky',
  zIndex: 1,
})
// The footer holds the actions alone, at its end.
export const publishFooterActions = style({ display: 'flex', justifyContent: 'flex-end' })
globalStyle(`${publishFooterActions} > *`, { marginBlockStart: 0 })
// The tools a reviewed source calls, one call path a line.
export const calledTools = style({
  display: 'grid',
  gap: 6,
  listStyle: 'none',
  margin: 0,
  padding: 0,
})
export const calledTool = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  minWidth: 0,
})
// A revision's number with what sets it apart, which wraps in a narrow column.
export const revisionCell = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  minWidth: 0,
})
