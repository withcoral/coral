import { globalStyle, style } from '@vanilla-extract/css'

import { utils } from '@/styles/utils'
import { theme } from '@/wax/theme/theme.css'

export const setupPage = style({ maxWidth: 1120, width: '100%', minWidth: 0 })
export const sourceGrid = style({
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 240px), 1fr))',
  gap: 12,
})
export const facts = style({
  display: 'grid',
  gridTemplateColumns: 'repeat(3, minmax(0, 1fr))',
  gap: 16,
  '@media': { '(max-width: 500px)': { gridTemplateColumns: '1fr' } },
})

export const callPath = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  minWidth: 0,
})
export const pills = style({ alignItems: 'center', display: 'flex', flexWrap: 'wrap', gap: 8 })
export const stack = style({ display: 'flex', flexDirection: 'column', gap: 16, minWidth: 0 })
// A step under the page title it heads: spaced as the page frame spaces its header.
export const rowActions = style({ display: 'flex', flexWrap: 'wrap', gap: 8 })
export const paneSummary = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  minWidth: 0,
})
export const paneSummaryActions = style({
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  marginInlineStart: 'auto',
})
export const field = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  maxWidth: 460,
  minWidth: 0,
})
export const choices = style({ display: 'flex', flexDirection: 'column', gap: 8 })
// A menu button in a stacked field sizes to its label, not to the field.
export const inlineControl = style({ alignSelf: 'flex-start', maxWidth: '100%' })
// A field whose value is a URL to copy: wider than a name, and the input takes the row.
export const wideField = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  maxWidth: 640,
  minWidth: 0,
})
export const grow = style({ flex: '1 1 auto', minWidth: 0 })
// A source address takes the width it is given: the dialog is wide enough.
export const sourceField = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  minWidth: 0,
})
// One way to add a custom provider: its fields framed together.
export const sourceKind = style({ gap: 16, minWidth: 0 })
// The account question compares two routes side by side, so it takes the step's
// full width, and each column holds the outcomes of one route.
export const accountQuestion = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  minWidth: 0,
})
export const accountColumn = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
})
// Two routes compared side by side need a clear gutter between them. Stacked on
// a narrow screen they need the same separation down the page. Unlayered, so
// this beats the grid gap Wax gives every radio group.
export const accountRoutes = style({ columnGap: 32, rowGap: 24 })

// A value Coral acts on as soon as you enter it: the action sits with the input.
export const fieldWithAction = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  minWidth: 0,
})
// The action keeps room for the word it shows while it works, so the input
// beside it does not resize when a read starts.
export const readAction = style({ flexShrink: 0, minWidth: 116 })
// Fixed text before a value, beside the input rather than over it.
export const prefixedField = style({ alignItems: 'center', display: 'flex', gap: 6, minWidth: 0 })
export const prefixText = style({ whiteSpace: 'nowrap' })
export const indent = style({
  borderInlineStart: `2px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  marginInlineStart: 8,
  paddingInlineStart: 16,
})
export const list = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  listStyle: 'none',
  margin: 0,
  padding: 0,
})

export const removalDetails = style({ display: 'flex', flexDirection: 'column', gap: 8 })
export const bulletList = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  margin: 0,
  paddingLeft: 20,
  listStyle: 'disc',
})

export const longValue = style({ overflowWrap: 'anywhere' })

export const emptyProviders = style({ display: 'flex', height: '100%' })
export const filterLabel = style({
  alignItems: 'center',
  display: 'flex',
  gap: 6,
  ...theme.typography.bodySmallStrong,
})
export const helpIcon = style({ alignItems: 'center', display: 'inline-flex' })
// The table scrolls, so the browser can focus it. While a keyboard cursor row
// shows where the keys act, the table draws no focus outline of its own.
export const providersTable = style({
  minHeight: 0,
  selectors: { '&:focus:has([data-active])': { outline: 'none' } },
})
// The policy column ends a step in from the table's edge.
globalStyle(`${providersTable} [role="row"] > :last-child`, { paddingInlineEnd: 24 })
// A no-match status runs its copy and its button on one centred line.
export const sectionStatus = style({
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
// The provider's policy counts, at the end of its pinned heading. They sit in
// the heading's label cell, not the policy column, so the column sizes to the
// rows' toggles and a changing count moves only the heading.
export const policySummary = style({
  alignItems: 'center',
  display: 'flex',
  flexShrink: 0,
  flexWrap: 'nowrap',
  gap: 4,
  marginInlineStart: 'auto',
})
// A count is a Wax toggle button; pressed, it reads as the active filter.
export const policySummaryItem = style({
  color: theme.content.secondary,
  ...theme.typography.bodySmall,
  selectors: {
    '&:hover, &[aria-pressed="true"]': { color: theme.content.primary },
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
export const toolName = style({
  color: theme.content.primary,
  minWidth: 0,
  overflow: 'hidden',
  textDecoration: 'none',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  ...theme.typography.bodySmall,
  selectors: {
    '&:focus-visible': { borderRadius: 4, outline: `2px solid ${theme.stroke.focused}` },
  },
})
export const toolDescriptionCell = style({
  display: 'flex',
  minWidth: 0,
})
export const toolDescriptionText = style({
  minWidth: 0,
  ...utils.boxClamp(2),
})
export const inlineTooltipTarget = style({ display: 'inline-flex' })
// The collapse-all toggle starts the column, above the headings' own toggles.
export const nameHeader = style({ alignItems: 'center', display: 'flex', gap: 8 })
// The column is as wide as its toggles; the header centres over them.
export const policyHeader = style({ display: 'flex', justifyContent: 'center' })
// One line: a wrapped outcome pill would change the row's height after a save.
export const policyCell = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'nowrap',
  gap: 6,
})
export const checkboxWrapper = style({ alignItems: 'center', display: 'flex' })

// Catalog rows: logo, name and one line, then the action.
// Two columns, so a card has room for its name beside two buttons. A column is
// never narrower than 320px or half the list, so a list too narrow for two
// has one, wherever the page puts it.
export const catalogList = style({
  display: 'grid',
  gap: 8,
  gridTemplateColumns: 'repeat(auto-fill, minmax(min(100%, max(320px, calc(50% - 4px))), 1fr))',
  listStyle: 'none',
  margin: 0,
  padding: 0,
})
// The buttons that add a catalog service from the providers rail, one per source.
export const railSources = style({ display: 'grid', gap: 8, minWidth: 0 })
// Wax cards in a grid: each fills its cell, so a row's cards share one height.
export const catalogItem = style({ display: 'flex', minWidth: 0 })
export const catalogCard = style({ height: '100%', minWidth: 0 })
// No match: the message fills the rows' space and sits in its middle.
export const catalogEmpty = style({
  alignItems: 'center',
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: 4,
  justifyContent: 'center',
  minHeight: 160,
  paddingInline: 16,
  textAlign: 'center',
})
export const catalogEmptyActions = style({
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  justifyContent: 'center',
  marginBlockStart: 12,
})
// What to do on the left; the count and the search on the right.
export const catalogToolbar = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 16,
  justifyContent: 'space-between',
})
export const catalogFind = style({
  alignItems: 'center',
  display: 'flex',
  flex: '0 1 auto',
  gap: 12,
  marginInlineStart: 'auto',
})
// The count is short; it never wraps or gives its width to the search.
export const catalogCount = style({ flexShrink: 0, whiteSpace: 'nowrap' })
export const catalogSearch = style({ flex: '0 1 320px', minWidth: 200 })

// In the launcher dialog the catalog takes the height the source form leaves,
// and its rows are the only thing that scrolls.
export const catalogPane = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: 16,
  minHeight: 0,
  minWidth: 0,
})
/**
 * The launcher dialog takes a definite height so its flex children have a
 * remainder to share: without one everything sizes to content and the list
 * has nothing to scroll. The source form and the footer keep their size, so
 * the remainder is the list's. Below roughly 600px of viewport there is no
 * remainder left and the popup scrolls too, which beats clipping the footer.
 */
export const addProviderPopup = style({
  height: 'min(860px, calc(100vh - 64px))',
  selectors: { '&&': { width: 880 } },
})
// Never let the list vanish entirely on a short viewport.
export const catalogScroll = style({ flex: '1 1 auto', minHeight: 120 })
export const catalogScrollContent = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  // Room for a focused row's outline, which sits outside the row.
  padding: 2,
  // The Wax scrollbar floats over the content's right edge (4px bar, 4px
  // margin), so the cards stop short of it.
  paddingInlineEnd: 14,
})
export const chosenRecipe = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
})
export const checklist = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  margin: 0,
  paddingInlineStart: 20,
})
// Wizard actions, the primary button last.
export const stepActions = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  justifyContent: 'flex-end',
})
// A step's title and its actions, the actions at the content's edge. It stays
// pinned while a long step scrolls, so its actions are always in reach.
export const stepHeader = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  justifyContent: 'space-between',
  minWidth: 0,
  position: 'sticky',
  top: 0,
  zIndex: 1,
  // Content scrolls beneath it, so it needs the page's opaque surface. The
  // padding gives the pinned row room; the margin keeps the unpinned layout.
  background: theme.surface.mainContent,
  paddingBlock: 12,
  marginBlock: -12,
})
export const stepTitle = style({
  alignItems: 'baseline',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  minWidth: 0,
})
export const destinationList = style({ display: 'flex', flexDirection: 'column', gap: 10 })
export const destination = style({ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 })
export const destinationLine = style({ display: 'flex', flexWrap: 'wrap', gap: '0 12px' })
export const orDivider = style({
  alignItems: 'center',
  display: 'flex',
  gap: 12,
  selectors: {
    '&::before, &::after': {
      background: theme.stroke.primary,
      blockSize: 1,
      content: '""',
      flex: 1,
    },
  },
})

/**
 * The two source panels share one grid cell. Only the chosen one shows, so the
 * block is as tall as its fields. The hidden panel stays mounted to keep what
 * was typed, and submits nothing: its own fields are disabled.
 */
export const sourceSlot = style({ display: 'grid', minWidth: 0 })
/** In a bounded dialog the chosen panel takes the height left, so the catalog can scroll. */
export const sourceFill = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: 16,
  minHeight: 0,
  minWidth: 0,
})
export const sourceTabsFill = style([sourceFill, { gap: 0 }])
// The tab list sits in a clipping scroll area: squeezed by a tall catalog, it
// would lose its bottom rule and the indicator on it.
globalStyle(`${sourceTabsFill} > :first-child`, { flexShrink: 0 })
export const sourceSlotFill = style([
  sourceSlot,
  { flex: '1 1 auto', gridTemplateRows: 'minmax(0, 1fr)', minHeight: 0 },
])
// In a form the tabs sit clear of the field under them by the usual small step.
export const sourceTabs = style({ marginBlockEnd: 8 })
export const sourcePanel = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  gridArea: '1 / 1',
  minWidth: 0,
  selectors: {
    '&[data-hidden]': { display: 'none' },
  },
})

// Configuration: the fields on the left, what Coral was given or discovered on
// the right. Only values the person cannot change belong in the read column.
export const configureSplit = style({
  alignItems: 'start',
  display: 'grid',
  columnGap: 48,
  gridTemplateColumns: 'repeat(2, minmax(0, 460px))',
  // The two columns take the page's width between them rather than huddling left.
  justifyContent: 'space-between',
  minWidth: 0,
  rowGap: 24,
  '@media': { '(max-width: 1040px)': { gridTemplateColumns: 'minmax(0, 1fr)' } },
})
// The one boundary on the form: what Coral calls the provider, above how it
// signs in. Spans both columns, so it divides the page and not a column.
export const groupRule = style({
  borderTop: `1px solid ${theme.stroke.secondary}`,
  gridColumn: '1 / -1',
})

// A group with nothing fixed beside it takes the whole width.
export const fullRow = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  gridColumn: '1 / -1',
  minWidth: 0,
})
// A full row that keeps to the left column's width, so it ends where the
// fields above it end instead of partway under the right column.
export const leftColumnWidth = style({
  maxWidth: 460,
  '@media': { '(max-width: 1040px)': { maxWidth: 'none' } },
})
export const readColumn = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  minWidth: 0,
})

export const twoUp = style({
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
  // An expanded server editor must not stretch the card beside it.
  alignItems: 'start',
  '@media': { '(max-width: 640px)': { gridTemplateColumns: 'minmax(0, 1fr)' } },
})

// The catalog reads as one panel: the search box sits with the list it filters.
export const toolsBrowser = style({
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 10,
  display: 'flex',
  flexDirection: 'column',
  minWidth: 0,
  overflow: 'hidden',
})
// The controls sit at the end of the row: the list is the thing being read,
// and the search box is what you reach for once reading has not found it.
export const toolsSearch = style({
  alignItems: 'center',
  borderBottom: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  justifyContent: 'flex-end',
  padding: 8,
})
export const toolsFind = style({ maxWidth: 260, minWidth: 160 })
// Nothing to list yet: what the empty panel offers under its own copy, kept
// in one centred column so a form, a notice and a button line up.
export const emptyActions = style({
  alignItems: 'center',
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  minWidth: 0,
  width: '100%',
})
// A Setup that ended: its actions in one centred row that wraps on a narrow screen.
export const endedActions = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  justifyContent: 'center',
})
export const toolsScroll = style({ padding: 4 })
// An empty panel keeps roughly the height it will have once it holds rows, and
// centres its message in that room rather than resting on the search bar.
// Full width, so its percentage-wide rows size against the panel.
export const toolsSkeleton = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 14,
  padding: 16,
  width: '100%',
})
export const toolsEmpty = style({
  alignItems: 'center',
  display: 'flex',
  justifyContent: 'center',
  minHeight: 260,
  padding: 16,
})

// Credential methods: a table of inputs and a preview of what Coral sends.
export const credentialTable = style({ maxWidth: 880 })
export const headerPreview = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  maxWidth: 880,
  minWidth: 0,
})

export const settingsUrl = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  margin: 0,
  minWidth: 0,
})
// The URL shortens so its copy button stays on the same line.
export const settingsUrlRow = style({
  alignItems: 'center',
  display: 'flex',
  gap: 4,
  minWidth: 0,
})
export const settingsUrlText = style({ flex: '0 1 auto', minWidth: 0, ...utils.boxClamp(1) })
// The source's actions, side by side under its facts.
export const settingsSourceActions = style({
  borderTop: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexWrap: 'wrap',
  gap: 8,
  paddingTop: 16,
})

// The provider name heads the page; the pencil sits right beside it.

// The scopes take the row; the separator picker keeps its width beside them.
export const scopesRow = style({
  display: 'flex',
  flexWrap: 'wrap',
  alignItems: 'flex-start',
  gap: 16,
  maxWidth: 640,
})
export const scopesInput = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
  flex: '1 1 280px',
  minWidth: 0,
})
export const scopesFormat = style({ display: 'flex', flexDirection: 'column', gap: 6 })
// The asterisk is drawn, not written, so it stays out of the label's text and
// its accessible name. The control's own `required` tells a screen reader.
export const requiredMark = style({
  marginLeft: '0.25em',
  color: theme.content.error,
  '::after': { content: '"*" / ""' },
})
