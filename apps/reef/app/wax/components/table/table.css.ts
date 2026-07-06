import { createVar, fallbackVar, style } from '@vanilla-extract/css'
import { recipe } from '@vanilla-extract/recipes'

import { animation, theme } from '@/wax/theme/theme.css'

// The fallback keeps cells styled as the default table style when they're
// rendered without a Table.Wrapper setting the var.
const createTableVar = (fallback: string) => {
  const ref = createVar()
  return { ref, value: fallbackVar(ref, fallback) }
}

const tableVars = {
  cellEdgePaddingInlineEnd: createTableVar('12px'),
  cellEdgePaddingInlineStart: createTableVar('12px'),
  cellPaddingBlock: createTableVar('12px'),
  cellPaddingInline: createTableVar('12px'),
  headerBorderTop: createTableVar('none'),
  headerEdgePaddingInlineEnd: createTableVar('12px'),
  headerEdgePaddingInlineStart: createTableVar('12px'),
  headerPaddingBlock: createTableVar('12px'),
  headerPaddingInline: createTableVar('12px'),
  rowHoverBackground: createTableVar(theme.surface.onMainContent),
}

export const wrapper = recipe({
  base: {
    border: 'none',
    borderRadius: 0,
    overflowX: 'auto',
  },
  defaultVariants: {
    tableStyle: 'default',
  },
  variants: {
    tableStyle: {
      compact: {
        vars: {
          [tableVars.cellEdgePaddingInlineEnd.ref]: '12px',
          [tableVars.cellEdgePaddingInlineStart.ref]: '12px',
          [tableVars.cellPaddingBlock.ref]: '6px',
          [tableVars.cellPaddingInline.ref]: '12px',
          [tableVars.headerBorderTop.ref]: 'none',
          [tableVars.headerEdgePaddingInlineEnd.ref]: '12px',
          [tableVars.headerEdgePaddingInlineStart.ref]: '12px',
          [tableVars.headerPaddingBlock.ref]: '6px',
          [tableVars.headerPaddingInline.ref]: '12px',
          [tableVars.rowHoverBackground.ref]: theme.surface.onMainContentSubtle,
        },
      },
      default: {
        vars: {
          [tableVars.cellEdgePaddingInlineEnd.ref]: '12px',
          [tableVars.cellEdgePaddingInlineStart.ref]: '12px',
          [tableVars.cellPaddingBlock.ref]: '12px',
          [tableVars.cellPaddingInline.ref]: '12px',
          [tableVars.headerBorderTop.ref]: 'none',
          [tableVars.headerEdgePaddingInlineEnd.ref]: '12px',
          [tableVars.headerEdgePaddingInlineStart.ref]: '12px',
          [tableVars.headerPaddingBlock.ref]: '12px',
          [tableVars.headerPaddingInline.ref]: '12px',
          [tableVars.rowHoverBackground.ref]: theme.surface.onMainContent,
        },
      },
    },
  },
})

export const table = style({
  borderCollapse: 'collapse',
  width: '100%',
})

export const thead = style({
  backgroundColor: theme.surface.card,
  position: 'sticky',
  top: 0,
  zIndex: 1,
})

export const th = style({
  ...theme.typography.bodyStrong,
  borderBottom: `1px solid ${theme.stroke.primary}`,
  borderTop: tableVars.headerBorderTop.value,
  color: theme.content.primary,
  paddingBlock: tableVars.headerPaddingBlock.value,
  paddingInline: tableVars.headerPaddingInline.value,
  textAlign: 'left',
  whiteSpace: 'nowrap',
  selectors: {
    '&:first-child': {
      paddingInlineStart: tableVars.headerEdgePaddingInlineStart.value,
    },
    '&:last-child': {
      paddingInlineEnd: tableVars.headerEdgePaddingInlineEnd.value,
    },
  },
})

export const tbody = style({})

export const tr = style({
  borderBottom: `1px solid ${theme.stroke.primary}`,
  transition: animation.colorTransition,
  selectors: {
    'tbody &:hover': {
      backgroundColor: tableVars.rowHoverBackground.value,
    },
  },
})

// Shared layout for both cell variants — they differ only by typography token.
const cellLayout = {
  color: theme.content.secondary,
  maxWidth: '250px',
  overflow: 'hidden',
  paddingBlock: tableVars.cellPaddingBlock.value,
  paddingInline: tableVars.cellPaddingInline.value,
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  selectors: {
    '&:first-child': {
      paddingInlineStart: tableVars.cellEdgePaddingInlineStart.value,
    },
    '&:last-child': {
      paddingInlineEnd: tableVars.cellEdgePaddingInlineEnd.value,
    },
  },
} as const

export const td = style({
  ...cellLayout,
  ...theme.typography.codeInline,
})

export const tdText = style({
  ...cellLayout,
  ...theme.typography.body,
})
