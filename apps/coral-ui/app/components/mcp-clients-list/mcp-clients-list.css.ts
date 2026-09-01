import { style } from '@vanilla-extract/css'

import { baseInput } from '@/wax/components/inputs/base-input.css'
import { theme } from '@/wax/theme/theme.css'
import { breakpoints } from '@/styles/theme.css'

const MOBILE_QUERY = `screen and (max-width: ${breakpoints.mobile})`

// Only the widths that change with the viewport live here; the columns
// themselves are described where the table is rendered.
export const WORKSPACE_WIDTH_PROPERTY = '--mcp-workspace-width'

export const responsiveWidths = style({
  vars: {
    [WORKSPACE_WIDTH_PROPERTY]: '260px',
  },
  '@media': {
    [MOBILE_QUERY]: {
      vars: {
        [WORKSPACE_WIDTH_PROPERTY]: '180px',
      },
    },
  },
})

export const selectTrigger = style({
  justifyContent: 'space-between',
})

export const selectMenu = style({
  maxWidth: 'calc(100vw - 32px)',
  width: '228px',
})

export const installPanel = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '20px',
})

export const installSelects = style({
  display: 'flex',
  flexWrap: 'wrap',
  gap: '16px',
})

export const installSelect = style({
  display: 'flex',
  flex: '0 1 260px',
  flexDirection: 'column',
  gap: '6px',
  minWidth: '180px',
})

export const installCommandContainer = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '6px',
})

// The command is read before it is copied, so it wraps to whatever height it
// needs instead of hiding its tail behind an ellipsis.
export const installField = style([
  baseInput,
  {
    overflow: 'hidden',
    padding: 0,
    position: 'relative',
  },
])

export const installCommand = style({
  boxSizing: 'border-box',
  color: theme.content.primary,
  margin: 0,
  overflowWrap: 'anywhere',
  paddingBlock: '12px',
  paddingInlineEnd: '52px',
  paddingInlineStart: '12px',
  whiteSpace: 'pre-wrap',
  width: '100%',
  ...theme.typography.code,
})

export const installCopyButton = style({
  insetBlockStart: '8px',
  insetInlineEnd: '8px',
  position: 'absolute',
})
