import { createVar, fallbackVar, globalStyle, recipe, style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

import {
  alignProperty,
  CELL_ALIGN_PROPERTY,
  CELL_HEIGHT_PX,
  CELL_JUSTIFY_PROPERTY,
  COLUMNS_PROPERTY,
  GROUP_DEPTH_PROPERTY,
  GROUP_HEADING_HEIGHT_PROPERTY,
  GROUP_HEADING_HEIGHT_PX,
  HEADING_HEIGHT_PX,
  justifyProperty,
  MAX_ALIGNED_COLUMNS,
  ROW_CONTROLS,
  ROWS_MAX_HEIGHT_PROPERTY,
  SCROLL_END_ROOM_PROPERTY,
} from './constants'

// The fallback keeps a cell rendered outside a Container styled as the default
// density rather than as an unset var.
const createTableVar = (fallback: string) => {
  const ref = createVar()
  return { ref, value: fallbackVar(ref, fallback) }
}

const tableVars = {
  cellHeight: createTableVar(`${CELL_HEIGHT_PX.default}px`),
  cellPaddingBlock: createTableVar('12px'),
  cellPaddingInline: createTableVar('12px'),
  groupHeadingHeight: createTableVar(`${GROUP_HEADING_HEIGHT_PX.default}px`),
  headingHeight: createTableVar(`${HEADING_HEIGHT_PX.default}px`),
  rowHoverBackground: createTableVar(theme.surface.onMainContent),
  // A scroll-layout row pads its cells itself. A compact one-line row stays at
  // the cell height, so it is as tall as a group heading that pins it.
  scrollRowPaddingBlock: createTableVar('8px'),
}

// What a cell resolved its alignment to. The text inside the cell reads it too:
// a custom property crosses that boundary, where `text-align` on a flex container
// would not reach the anonymous box it wraps its text in.
const alignVar = createVar()
const justifyVar = createVar()

const groupHeadingFirstCellInset = `calc(${tableVars.cellPaddingInline.value} * (var(${GROUP_DEPTH_PROPERTY}, 0) + 1))`
const groupHeadingFirstCellIndent = { paddingInlineStart: groupHeadingFirstCellInset } as const

// One rule per column position, handing that column's value to whatever cell
// lands there. Position is the only thing CSS can match on, and it is what the
// grid places the cell by, so the two can never disagree. The cell's own override
// comes first in the chain, so a prop beats its column without a specificity war.
const columnAlignSelectors: Record<string, { vars: Record<string, string> }> = Object.fromEntries(
  Array.from({ length: MAX_ALIGNED_COLUMNS }, (_, index) => [
    `&:nth-child(${index + 1})`,
    {
      vars: {
        [alignVar]: `var(${CELL_ALIGN_PROPERTY}, var(${alignProperty(index + 1)}, left))`,
        [justifyVar]: `var(${CELL_JUSTIFY_PROPERTY}, var(${justifyProperty(index + 1)}, flex-start))`,
      },
    },
  ]),
)

// The provider tools table's chrome and scroll behavior, shared by inspectors.
// The reading controller owns anchoring; native anchoring otherwise competes with sparse windows.
export const anchoredTable = style({ overflowAnchor: 'none' })

export const scrollTable = style({
  // Sticky rows stack inside the table, so the scroll area's scrollbars,
  // which follow it, paint above them.
  isolation: 'isolate',
  position: 'relative',
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  minBlockSize: 0,
  minWidth: 0,
  overscrollBehavior: 'contain',
  paddingBlockEnd: `var(${SCROLL_END_ROOM_PROPERTY}, 0px)`,
  selectors: {
    // The horizontal scrollbar's 20px hit area overlays the port's foot, so
    // the last row ends above it and keeps its clicks.
    '&[data-has-overflow-x]': {
      paddingBlockEnd: `max(20px, var(${SCROLL_END_ROOM_PROPERTY}, 0px))`,
    },
  },
})

// One grid owns every column width. Head, Body, and Row inherit those tracks
// through `subgrid`, so a column stays the same width in the heading and in
// every row without any of them being told how wide it is.
const CARD_RADIUS = 10

export const container = recipe({
  base: {
    alignContent: 'start',
    display: 'grid',
    // A flex parent must scroll the complete table, not shrink and clip its rows.
    flexShrink: 0,
    gridTemplateColumns: `var(${COLUMNS_PROPERTY}, auto)`,
  },
  defaultVariants: {
    density: 'default',
    layout: 'auto',
    variant: 'plain',
  },
  variants: {
    density: {
      compact: {
        vars: {
          [tableVars.cellHeight.ref]: `${CELL_HEIGHT_PX.compact}px`,
          [tableVars.cellPaddingBlock.ref]: '6px',
          [tableVars.cellPaddingInline.ref]: '12px',
          [tableVars.groupHeadingHeight.ref]:
            `var(${GROUP_HEADING_HEIGHT_PROPERTY}, ${GROUP_HEADING_HEIGHT_PX.compact}px)`,
          [tableVars.headingHeight.ref]: `${HEADING_HEIGHT_PX.compact}px`,
          [tableVars.rowHoverBackground.ref]: theme.surface.onMainContentSubtle,
          [tableVars.scrollRowPaddingBlock.ref]: '4px',
        },
      },
      // One line of small text with room around it, for a list read row by row.
      comfortable: {
        vars: {
          [tableVars.cellHeight.ref]: `${CELL_HEIGHT_PX.comfortable}px`,
          [tableVars.cellPaddingBlock.ref]: '8px',
          [tableVars.cellPaddingInline.ref]: '12px',
          [tableVars.groupHeadingHeight.ref]:
            `var(${GROUP_HEADING_HEIGHT_PROPERTY}, ${GROUP_HEADING_HEIGHT_PX.comfortable}px)`,
          [tableVars.headingHeight.ref]: `${HEADING_HEIGHT_PX.comfortable}px`,
          [tableVars.rowHoverBackground.ref]: theme.surface.onMainContentSubtle,
          [tableVars.scrollRowPaddingBlock.ref]: '5px',
        },
      },
      default: {
        vars: {
          [tableVars.cellHeight.ref]: `${CELL_HEIGHT_PX.default}px`,
          [tableVars.cellPaddingBlock.ref]: '12px',
          [tableVars.cellPaddingInline.ref]: '12px',
          [tableVars.groupHeadingHeight.ref]:
            `var(${GROUP_HEADING_HEIGHT_PROPERTY}, ${GROUP_HEADING_HEIGHT_PX.default}px)`,
          [tableVars.headingHeight.ref]: `${HEADING_HEIGHT_PX.default}px`,
          [tableVars.rowHoverBackground.ref]: theme.surface.onMainContent,
          [tableVars.scrollRowPaddingBlock.ref]: '8px',
        },
      },
    },
    layout: {
      // The Wax scroll area around it hides the native scrollbars.
      scroll: {
        flex: '1 1 auto',
        minHeight: 0,
        overflow: 'auto',
      },
      // Scrolling sideways makes this a scroll port on both axes, so a sticky
      // heading pins to a box with no scroll range of its own — nowhere.
      auto: {
        overflowX: 'auto',
        overflowY: 'hidden',
      },
      // `clip` keeps the rows inside the chrome without becoming that port, which
      // leaves the heading free to pin to the ancestor that really scrolls.
      fixed: {
        overflow: 'clip',
      },
    },
    variant: {
      card: {
        background: theme.surface.card,
        border: `1px solid ${theme.stroke.secondary}`,
        borderRadius: CARD_RADIUS,
      },
      plain: {},
    },
  },
})

// Marks a card table, so its last row can follow the card's rounded corners.
export const cardTable = style({})

const rowGroup = {
  display: 'grid',
  gridColumn: '1 / -1',
  gridTemplateColumns: 'subgrid',
} as const

export const head = style({
  ...rowGroup,
  // `floating` rather than `card`: card is 5% white in the dark theme, so a pinned
  // heading would let the rows scroll straight through it. Both are white in the
  // light theme, which is why the bug only shows in one of them.
  backgroundColor: theme.surface.floating,
  borderBottom: `1px solid ${theme.stroke.primary}`,
  position: 'sticky',
  top: 0,
  zIndex: 3,
  selectors: {
    [`${scrollTable} > &`]: {
      backgroundColor: theme.surface.mainContent,
      borderBottom: `1px solid ${theme.stroke.secondary}`,
    },
  },
})

export const scrollRows = style({})

// The heading keeps its height; the rows' track gives up the rest, down to nothing.
export const boundedRows = style({
  flex: '0 1 auto',
  gridTemplateRows: 'auto minmax(0, 1fr)',
  minHeight: 0,
})

export const bodyScrollArea = style({
  ...rowGroup,
  selectors: {
    [`${scrollRows} > &`]: {
      maxHeight: `var(${ROWS_MAX_HEIGHT_PROPERTY})`,
    },
    [`${boundedRows} > &`]: { minHeight: 0 },
  },
})

export const body = style({
  ...rowGroup,
  selectors: {
    // Only a capped body holds a scroll of its own. Everywhere else the port is
    // there but empty, and containing its overscroll would keep the wheel from
    // reaching whatever scrolls outside the table.
    [`:not(${scrollRows}) > ${bodyScrollArea} > &`]: {
      overscrollBehavior: 'auto',
    },
    // A bounded body is the scroll port and has a fixed height. Its `auto` rows
    // would shrink to their minimum height to fit, and a wrapped cell would
    // spill over the next row. Each row keeps its content's height instead.
    [`${boundedRows} > ${bodyScrollArea} > &`]: {
      gridAutoRows: 'max-content',
    },
  },
})

export const group = style(rowGroup)

// A nested group stacks as one layer, so its heading slides under its parent's
// heading as its rows run out, instead of over it.
export const nestedGroup = style({ position: 'relative', zIndex: 0 })

export const windowRowHeight = createVar()
export const windowGap = style({ gridColumn: '1 / -1', minWidth: 0, overflowAnchor: 'none' })

// The scroll area takes the table's place in its flex parent.
export const scrollArea = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  minHeight: 0,
  minWidth: 0,
})
// A row that opens its detail when clicked.
export const interactiveRow = style({ cursor: 'pointer' })
// The block padding a scroll-layout row has, for a cell that reaches into it.
const rowPaddingBlock = createVar()
// A scroll-layout row's inline padding and column gap, which a selection cell reaches into.
const scrollRowPaddingInline = 12
const scrollRowGap = 8

export const row = style({
  ...rowGroup,
  transition: 'background-color 0.1s ease',
  selectors: {
    [`${body} > &, ${group} > &`]: {
      borderBottom: `1px solid ${theme.stroke.primary}`,
      minHeight: tableVars.cellHeight.value,
    },
    // Only a row that acts shows hover: it opens something, or holds a control.
    [[body, group]
      .flatMap((parent) => [
        `${parent} > &${interactiveRow}:hover`,
        `${parent} > &:has(${ROW_CONTROLS}):hover`,
      ])
      .join(', ')]: {
      backgroundColor: tableVars.rowHoverBackground.value,
    },
    [`${body} > &:last-child, ${group} > &:last-child`]: {
      borderBottom: 'none',
    },
    // The card's radius less its border, so a highlight's corners match the card's.
    // The table's last row can sit in a group, or in a group nested in one.
    [[
      `${cardTable} ${body}:last-child > &:last-child`,
      `${cardTable} ${body}:last-child > ${group}:last-child > &:last-child`,
      `${cardTable} ${body}:last-child > ${group}:last-child > ${group}:last-child > &:last-child`,
    ].join(', ')]: {
      borderEndEndRadius: CARD_RADIUS - 1,
      borderEndStartRadius: CARD_RADIUS - 1,
    },
    [`${head} > &`]: {
      minHeight: tableVars.headingHeight.value,
    },
    [`${scrollTable} &`]: {
      alignItems: 'center',
      gap: scrollRowGap,
      vars: { [rowPaddingBlock]: tableVars.scrollRowPaddingBlock.value },
      paddingBlock: rowPaddingBlock,
      paddingInline: scrollRowPaddingInline,
      borderBottomColor: theme.stroke.secondary,
    },
    // Group headings pin below the head by its height, so it is exact.
    [`${scrollTable} ${head} > &`]: {
      backgroundColor: theme.surface.mainContent,
      vars: { [rowPaddingBlock]: '0px' },
      height: tableVars.headingHeight.value,
      minHeight: tableVars.headingHeight.value,
      color: theme.content.secondary,
      ...theme.typography.bodySmallStrong,
    },
  },
})

export const selectedRow = style({
  backgroundColor: theme.surface.onMainContent,
  boxShadow: `inset 3px 0 ${theme.content.primary}`,
})

// The keyboard cursor: an outline, so it reads apart from an open row's edge bar.
const cursor = {
  backgroundColor: theme.surface.onMainContentSubtle,
  boxShadow: `inset 0 0 0 2px ${theme.stroke.focused}`,
}
// The row's own focus ring would repeat the cursor's outline. Forced colors drop
// the shadow and the background, but they show a transparent outline.
export const activeRow = style({
  ...cursor,
  outline: '2px solid transparent',
  outlineOffset: -2,
})
// A table that takes focus shows it, unless a cursor row already shows where the keys act.
export const focusableTable = style({
  selectors: {
    '&:focus-visible': { outline: `2px solid ${theme.stroke.focused}`, outlineOffset: -2 },
    '&:focus-visible:has([data-active])': { outline: 'none' },
  },
})

// The marker overlays the existing edge without moving columns or replacing selection.
export const markedRow = style({
  position: 'relative',
  '::before': {
    content: '""',
    position: 'absolute',
    insetBlock: 0,
    insetInlineStart: 0,
    width: 3,
    pointerEvents: 'none',
    backgroundColor: theme.stroke.secondary,
  },
  selectors: {
    '&[data-status="success"]::before': { backgroundColor: theme.content.success },
    '&[data-status="error"]::before': { backgroundColor: theme.content.error },
    '&[data-status="warning"]::before': { backgroundColor: theme.content.warning },
  },
})

// `Table.Container` marks a table whose group headings are prominent. A
// selected heading keeps its selection tint instead.
export const prominentGroupHeadings = style({})

export const groupHeading = recipe({
  base: {
    backgroundColor: theme.surface.mainContent,
    borderBottom: `1px solid ${theme.stroke.secondary}`,
    borderTop: `1px solid ${theme.stroke.secondary}`,
    height: tableVars.groupHeadingHeight.value,
    maxHeight: tableVars.groupHeadingHeight.value,
    minHeight: tableVars.groupHeadingHeight.value,
    overflow: 'hidden',
    position: 'sticky',
    top: `calc(${tableVars.headingHeight.value} + var(${GROUP_DEPTH_PROPERTY}, 0) * ${tableVars.groupHeadingHeight.value})`,
    selectors: {
      [`${group} > &:hover`]: { backgroundColor: theme.surface.mainContent },
      // A collapsed heading is its group's last row. It keeps its border's
      // width, so its content does not move, and shows no line above the next.
      [`${group} > &:last-child`]: { borderBottom: '1px solid transparent' },
      // A nested heading pins below this one by its height, so it is exact.
      [`${scrollTable} ${group} > &`]: { vars: { [rowPaddingBlock]: '0px' } },
      // A record heading keeps its selection and status marker, and still pins.
      // Compound selectors win on specificity; `selectedRow` and `markedRow` are
      // declared above only so this rule can reference them.
      // The selection tint is translucent, so it lies over the opaque heading colour:
      // rows scroll under a pinned heading and must not show through it.
      [`&${selectedRow}, ${group} > &${selectedRow}:hover`]: {
        backgroundColor: theme.surface.mainContent,
        backgroundImage: `linear-gradient(${theme.surface.onMainContent}, ${theme.surface.onMainContent})`,
      },
      [`&${markedRow}`]: { position: 'sticky' },
    },
  },
  defaultVariants: {
    depth: 'top',
  },
  variants: {
    depth: {
      nested: {
        zIndex: 1,
        selectors: {
          [`${prominentGroupHeadings} &:not(${selectedRow})`]: {
            backgroundImage: `linear-gradient(${theme.surface.onMainContentSubtle}, ${theme.surface.onMainContentSubtle})`,
          },
        },
      },
      top: {
        backgroundImage: `linear-gradient(${theme.surface.onMainContentSubtle}, ${theme.surface.onMainContentSubtle})`,
        zIndex: 2,
        selectors: {
          [`${prominentGroupHeadings} &:not(${selectedRow})`]: {
            backgroundImage: `linear-gradient(${theme.surface.onMainContent}, ${theme.surface.onMainContent})`,
          },
        },
      },
    },
  },
})

export const groupHeadingFirstCell = style({
  ...groupHeadingFirstCellIndent,
})

// A head cell that starts with a control for every group, such as a collapse-all
// toggle. It indents as a top-level group heading does, so the two toggles line up.
export const groupToggleHeading = style({ paddingInlineStart: tableVars.cellPaddingInline.value })

// A cell is the box; the text inside it is a separate element. Truncation needs
// a block to apply to, and the box is a flex row so that a control dropped in a
// cell still lines up with the text in its neighbours.
export const cell = recipe({
  base: {
    alignItems: 'center',
    display: 'flex',
    justifyContent: fallbackVar(justifyVar, 'flex-start'),
    minWidth: 0,
    paddingInline: tableVars.cellPaddingInline.value,
    selectors: {
      ...columnAlignSelectors,
      [`&${groupHeadingFirstCell}`]: groupHeadingFirstCellIndent,
      [`${scrollTable} &`]: { paddingInline: 0 },
      [`${scrollTable} &${groupHeadingFirstCell}`]: groupHeadingFirstCellIndent,
    },
  },
  defaultVariants: {
    actions: false,
    control: false,
    fullWidth: false,
    wrap: false,
  },
  variants: {
    // An input brings its own padding, so the cell gives up its own to keep the
    // input's text in line with the heading.
    control: {
      false: {},
      true: { paddingInline: 4 },
    },
    fullWidth: {
      false: {},
      true: { gridColumn: '1 / -1' },
    },
    // Row actions show on the row the pointer or focus is in. A touch screen has
    // no hover, so they show there always.
    actions: {
      false: {},
      true: {
        // The button brings its own padding, and its hover fills it.
        paddingInline: 0,
        opacity: 0,
        transition: 'opacity 120ms ease',
        selectors: { [`${row}:hover &, ${row}:focus-within &`]: { opacity: 1 } },
        '@media': { '(hover: none)': { opacity: 1 } },
      },
    },
    wrap: {
      false: {},
      // A wrapping cell grows past the row height, so it aligns to the top and
      // pays for its own vertical padding.
      true: {
        alignItems: 'flex-start',
        paddingBlock: tableVars.cellPaddingBlock.value,
      },
    },
  },
})

export const cellText = recipe({
  base: {
    color: theme.content.secondary,
    flex: '1 1 auto',
    minWidth: 0,
    // Set on the cell, inherited here: a custom property crosses the boundary
    // that `text-align` on a flex container cannot.
    textAlign: fallbackVar(alignVar, 'left'),
  },
  defaultVariants: {
    actions: false,
    control: false,
    mono: false,
    wrap: false,
  },
  variants: {
    mono: {
      false: theme.typography.body,
      true: theme.typography.codeInline,
    },
    wrap: {
      false: {
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
      },
      true: {
        overflowWrap: 'anywhere',
        whiteSpace: 'normal',
      },
    },
    // An icon button is a flex box, which `text-align` does not centre. Its hover
    // must not be clipped, so this comes after `wrap` to override the overflow.
    actions: {
      false: {},
      true: { display: 'flex', justifyContent: 'center', overflow: 'visible' },
    },
    // A control draws its focus ring outside its box, which a clip would cut.
    control: {
      false: {},
      true: { overflow: 'visible' },
    },
  },
})

// A status row stands in for the rows a table has none of, so there is nothing
// under the pointer to take hold of. Holding the hover colour at nothing leaves
// the hover rule on `row` with no exception to make.
// Rows a virtualizer left unmounted, collapsed to their height.
export const virtualSpacer = style({
  pointerEvents: 'none',
  vars: {
    [tableVars.rowHoverBackground.ref]: 'transparent',
  },
})

// Rows that do nothing when clicked take no hover colour, so none of them looks
// like it opens something. Declared after `container`, so it wins over its density.
export const staticRows = style({
  vars: {
    [tableVars.rowHoverBackground.ref]: 'transparent',
  },
})

export const statusRow = style({
  vars: {
    [tableVars.rowHoverBackground.ref]: 'transparent',
  },
})

// Marks a `scope="table"` status, which a table shows only in place of all its rows.
export const tableStatusRow = style({})

// A scroll table fills the height its parent gives it. A table-wide status, or
// the body that holds it, comes straight after the head, so that track takes the
// spare height and the status sits in the middle.
globalStyle(`${scrollTable}:has(${tableStatusRow})`, {
  alignContent: 'stretch',
  gridTemplateRows: 'auto 1fr',
})

// The `align` prop on the cell centres the text: it reaches the nested text
// element through the alignment custom property, where `text-align` here would
// lose to that element's own rule.
export const statusCell = recipe({
  base: {
    paddingInline: '12px',
  },
  variants: {
    scope: {
      // The cells' own padding: a status reads as one more row, as tall as the rest.
      table: { paddingBlock: tableVars.cellPaddingBlock.value },
      // The scroll-layout row already pads its cells; one line then reaches
      // the height of an ordinary row.
      group: { paddingBlock: 0 },
    },
  },
})

export const heading = style({
  alignItems: 'center',
  display: 'flex',
  justifyContent: fallbackVar(justifyVar, 'flex-start'),
  minWidth: 0,
  paddingInline: tableVars.cellPaddingInline.value,
  selectors: {
    ...columnAlignSelectors,
    [`&${groupHeadingFirstCell}`]: groupHeadingFirstCellIndent,
    [`${scrollTable} &`]: { paddingInline: 0 },
    [`${scrollTable} &${groupHeadingFirstCell}`]: groupHeadingFirstCellIndent,
  },
})

export const headingText = style({
  ...theme.typography.bodyStrong,
  color: theme.content.primary,
  flex: '1 1 auto',
  minWidth: 0,
  overflow: 'hidden',
  textAlign: fallbackVar(alignVar, 'left'),
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  selectors: {
    [`${scrollTable} &`]: {
      color: theme.content.secondary,
      ...theme.typography.bodySmallStrong,
    },
  },
})

// The whole row is the button, so it fills the cell and draws no chrome of its own.
export const addRowCell = style({ padding: 0, position: 'relative' })
export const addRow = style({
  alignItems: 'center',
  background: 'transparent',
  border: 0,
  color: theme.content.secondary,
  cursor: 'pointer',
  display: 'flex',
  gap: 8,
  inset: 0,
  // In line with the text of an input in a control cell.
  paddingInline: 16,
  position: 'absolute',
  ...theme.typography.body,
  selectors: {
    '&:hover': { color: theme.content.primary },
    '&:focus-visible': { outline: `2px solid ${theme.input.stroke.focus}`, outlineOffset: -2 },
  },
})

// A load in flight at an end of the table: a small spinner, not a control.
export const loadMoreStatus = style({
  alignItems: 'center',
  display: 'inline-flex',
})

// The row's own link: the row's hover already shows it opens, so the text keeps
// its colour. The cell clips overflow, so the row draws the focus, not the link.
export const rowLink = style({
  color: 'inherit',
  minWidth: 0,
  outline: 'none',
  overflow: 'hidden',
  textDecoration: 'none',
  textOverflow: 'ellipsis',
})
// A row with `to` shows its link's keyboard focus as the keyboard cursor.
export const linkRow = style({
  selectors: {
    [`&:has(${rowLink}:focus-visible)`]: cursor,
  },
})

// A row's selection checkbox. In a scroll-layout table the cell reaches into
// the row's start and block padding and the gap after it, and its label
// pads the box back into place. A click near the box then checks it, not the row.
export const selectionCell = style({
  alignSelf: 'stretch',
  display: 'flex',
  selectors: {
    [`${scrollTable} &`]: {
      marginBlock: `calc(-1 * ${rowPaddingBlock})`,
      marginInline: `-${scrollRowPaddingInline}px -${scrollRowGap}px`,
      // Its column aligns it at the start, so it takes the extra width itself.
      width: `calc(100% + ${scrollRowPaddingInline + scrollRowGap}px)`,
    },
  },
})
globalStyle(`${scrollTable} ${selectionCell} > label`, {
  paddingBlock: rowPaddingBlock,
  paddingInline: `${scrollRowPaddingInline}px ${scrollRowGap}px`,
})
