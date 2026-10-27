import { createContainer, globalStyle, style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'
import { visuallyHiddenRules } from '@/wax/components/visually-hidden.css'

export const layout = style({
  display: 'grid',
  gridTemplateColumns: '260px minmax(0, 1fr)',
  height: '100%',
  columnGap: 0,
  minHeight: 0,
  minWidth: 0,
  width: '100%',
  '@media': { '(max-width: 760px)': { gridTemplateColumns: '1fr' } },
})
// Beside the content, the content's own border divides the two, so the rail
// draws none: two rules a few pixels apart read as a mistake.
export const rail = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  minHeight: 0,
  minWidth: 0,
  paddingInlineEnd: 16,
  '@media': {
    '(max-width: 760px)': {
      borderBlockEnd: `1px solid ${theme.stroke.secondary}`,
      paddingBlockEnd: 16,
      paddingInlineEnd: 0,
    },
  },
})
export const filterField = style({ display: 'flex', flexDirection: 'column', minWidth: 0 })
// The rail owns its own scroll port; the page ancestor clips overflow.
export const railScroll = style({ flex: '1 1 auto', minHeight: 0 })
export const railList = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
})
// The empty rail's message, centered below the entries, with any action
// below it at the rail's width, so every action button has one size.
export const railEmpty = style({
  alignItems: 'center',
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  paddingBlock: 24,
  paddingInline: 8,
  // A long filter breaks inside the message rather than past the rail.
  overflowWrap: 'anywhere',
  textAlign: 'center',
  textWrap: 'balance',
})
export const railEmptyAction = style({
  alignSelf: 'stretch',
  display: 'grid',
  marginBlockStart: 12,
  minWidth: 0,
})
// The action shrinks to the rail, where a long label truncates.
globalStyle(`${railEmptyAction} > *`, { maxWidth: '100%', minWidth: 0 })
// One entry in the rail: the link that scrolls the table to it, and any
// trailing control, which a caller can reveal on hover, focus, or current.
export const railItem = style({
  alignItems: 'center',
  borderRadius: 8,
  display: 'flex',
  gap: 2,
  paddingInlineEnd: 4,
  selectors: {
    '&:hover': { background: theme.sidebar.button.hover },
    '&[data-current="page"]': { background: theme.surface.onMainContentSubtle },
  },
})
export const railLink = style({
  borderRadius: 8,
  flex: '1 1 auto',
  gap: 10,
  justifyContent: 'flex-start',
  minWidth: 0,
  paddingBlock: 6,
  paddingInline: 8,
  // The entry, not the link, shows hover and focus, so both cover the
  // trailing controls too.
  selectors: {
    [`${railItem} &:hover`]: { background: 'transparent' },
    [`${railItem} &:focus-visible`]: { outline: 'none' },
  },
})
// Drawn inside the entry so the rail's scroll port does not clip it.
globalStyle(`${railItem}:has(> ${railLink}:focus-visible)`, {
  outline: `1px solid ${theme.button.primary.focus}`,
  outlineOffset: -1,
})
export const railName = style({
  flex: '1 1 auto',
  minWidth: 0,
  overflow: 'hidden',
  textAlign: 'start',
  textOverflow: 'ellipsis',
})
// Metadata, so it reads as quietly as the table's own counts.
export const railCount = style({
  color: theme.content.tertiary,
  flexShrink: 0,
  fontVariantNumeric: 'tabular-nums',
  marginInlineStart: 'auto',
  ...theme.typography.bodySmall,
})
export const railTrailing = style({ alignItems: 'center', display: 'flex', flexShrink: 0 })
// The controls show on the current entry and while an entry has the pointer or
// keyboard focus. Hidden, they keep their room, so a name does not reflow on
// hover, and Tab still reaches them.
globalStyle(`${railItem}:not(:hover, :has(:focus-visible), [data-current]) ${railTrailing}`, {
  opacity: 0,
})
export const main = style({
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  minHeight: 0,
  minWidth: 0,
  paddingInlineStart: 16,
  // Below the rail, the content starts on the rail's edge.
  '@media': { '(max-width: 760px)': { paddingInlineStart: 0 } },
})
// The rail's gap, so the first rail entry and the table start on one line.
export const stack = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: 16,
  minHeight: 0,
  minWidth: 0,
})
export const toolbar = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  justifyContent: 'space-between',
  minWidth: 0,
})
export const toolbarGroup = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 12,
  minWidth: 0,
})
// A toolbar with an action lays itself out by its own width, since the rail
// beside it narrows the column well before the viewport does. Without one,
// a toolbar is no container, so a card's toolbar keeps sizing to its content.
const toolbarContainer = createContainer()
// The controls take their own row below the title.
const STACKED_TOOLBAR = `${toolbarContainer} (max-width: 640px)`
// The action shows only its icon, so the filter keeps room to type in.
const COMPACT_TOOLBAR = `${toolbarContainer} (max-width: 420px)`
export const toolbarWithAction = style({
  containerName: toolbarContainer,
  containerType: 'inline-size',
})
// The trailing group keeps its controls on one line, at the end. When the row
// has no room for them at full width, the whole group moves below the title,
// so the picker never stacks over the filter beside a title centred on both.
// With an action, the group stays beside the title while the filter can
// shrink, and the action never takes a line of its own; below that, the
// group takes its own row and the filter fills it.
export const toolbarControls = style({
  flex: '1 1 auto',
  flexWrap: 'nowrap',
  justifyContent: 'flex-end',
  selectors: { [`${toolbarWithAction} > &`]: { flexBasis: 0 } },
  '@container': {
    [STACKED_TOOLBAR]: { selectors: { [`${toolbarWithAction} > &`]: { flexBasis: '100%' } } },
    // Too narrow for the picker, the filter and the action on one line: the
    // filter and the action wrap below the picker, never off the edge.
    [COMPACT_TOOLBAR]: {
      selectors: {
        [`${toolbarWithAction} > &`]: { flexWrap: 'wrap', justifyContent: 'flex-start' },
      },
    },
  },
})
export const toolbarFilterAndAction = style({
  alignItems: 'center',
  display: 'flex',
  flex: '0 1 auto',
  gap: 12,
  '@container': {
    [STACKED_TOOLBAR]: { flexGrow: 1 },
    // Wraps at its least width (the filter's least and the action's), not
    // its preferred one.
    [COMPACT_TOOLBAR]: { flexBasis: 0 },
  },
})
export const toolbarAction = style({ display: 'flex', flexShrink: 0 })
// For the action's label: assistive technology still reads it as the
// action's name when a narrow toolbar hides it.
export const toolbarActionLabel = style({
  '@container': {
    [COMPACT_TOOLBAR]: visuallyHiddenRules,
  },
})
export const toolbarTitle = style({ alignItems: 'center', display: 'flex', gap: 8, minWidth: 0 })
// The basis decides when the group wraps; on its own line the filter then
// shrinks to fit a phone.
export const toolbarFilter = style({
  flex: '0 1 360px',
  minWidth: 120,
  '@container': { [STACKED_TOOLBAR]: { flexGrow: 1 } },
})
// In a narrow column the filter takes the rest of the row, so it stays beside its picker.
export const toolbarFilterFill = style({ flex: '1 1 160px', minWidth: 0 })
