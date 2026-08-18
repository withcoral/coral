import { createVar, fallbackVar, style } from '@vanilla-extract/css'
import { recipe } from '@vanilla-extract/recipes'

import { theme, zIndex } from '@/wax/theme/theme.css'

import {
  alignProperty,
  CELL_ALIGN_PROPERTY,
  CELL_HEIGHT_PX,
  CELL_JUSTIFY_PROPERTY,
  COLUMNS_PROPERTY,
  HEADING_HEIGHT_PX,
  justifyProperty,
  MAX_ALIGNED_COLUMNS,
  ROWS_MAX_HEIGHT_PROPERTY,
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
  headingHeight: createTableVar(`${HEADING_HEIGHT_PX.default}px`),
  rowHoverBackground: createTableVar(theme.surface.onMainContent),
}

// What a cell resolved its alignment to. The text inside the cell reads it too:
// a custom property crosses that boundary, where `text-align` on a flex container
// would not reach the anonymous box it wraps its text in.
const alignVar = createVar()
const justifyVar = createVar()

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

// One grid owns every column width. Head, Body, and Row inherit those tracks
// through `subgrid`, so a column stays the same width in the heading and in
// every row without any of them being told how wide it is.
export const container = recipe({
  base: {
    alignContent: 'start',
    display: 'grid',
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
          [tableVars.headingHeight.ref]: `${HEADING_HEIGHT_PX.compact}px`,
          [tableVars.rowHoverBackground.ref]: theme.surface.onMainContentSubtle,
        },
      },
      default: {
        vars: {
          [tableVars.cellHeight.ref]: `${CELL_HEIGHT_PX.default}px`,
          [tableVars.cellPaddingBlock.ref]: '12px',
          [tableVars.cellPaddingInline.ref]: '12px',
          [tableVars.headingHeight.ref]: `${HEADING_HEIGHT_PX.default}px`,
          [tableVars.rowHoverBackground.ref]: theme.surface.onMainContent,
        },
      },
    },
    layout: {
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
        borderRadius: '10px',
      },
      plain: {},
    },
  },
})

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
  zIndex: zIndex.raised,
})

export const scrollRows = style({})

export const bodyScrollArea = style({
  ...rowGroup,
  selectors: {
    [`${scrollRows} > &`]: {
      maxHeight: `var(${ROWS_MAX_HEIGHT_PROPERTY})`,
    },
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
  },
})

export const row = style({
  ...rowGroup,
  transition: 'background-color 0.1s ease',
  selectors: {
    [`${body} > &`]: {
      borderBottom: `1px solid ${theme.stroke.primary}`,
      minHeight: tableVars.cellHeight.value,
    },
    [`${body} > &:hover`]: {
      backgroundColor: tableVars.rowHoverBackground.value,
    },
    [`${body} > &:last-child`]: {
      borderBottom: 'none',
    },
    [`${head} > &`]: {
      minHeight: tableVars.headingHeight.value,
    },
  },
})

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
    selectors: columnAlignSelectors,
  },
  defaultVariants: {
    fullWidth: false,
    wrap: false,
  },
  variants: {
    fullWidth: {
      false: {},
      true: { gridColumn: '1 / -1' },
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
  },
})

// A status row stands in for the rows a table has none of, so there is nothing
// under the pointer to take hold of. Holding the hover colour at nothing leaves
// the hover rule on `row` with no exception to make.
export const statusRow = style({
  vars: {
    [tableVars.rowHoverBackground.ref]: 'transparent',
  },
})

// The `align` prop on the cell centres the text: it reaches the nested text
// element through the alignment custom property, where `text-align` here would
// lose to that element's own rule.
export const statusCell = style({
  paddingBlock: '24px',
  paddingInline: '12px',
})

export const heading = style({
  alignItems: 'center',
  display: 'flex',
  justifyContent: fallbackVar(justifyVar, 'flex-start'),
  minWidth: 0,
  paddingInline: tableVars.cellPaddingInline.value,
  selectors: columnAlignSelectors,
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
})
