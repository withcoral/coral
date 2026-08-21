import { globalStyle, style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'

const MOBILE_QUERY = `screen and (max-width: ${breakpoints.mobile})`

// Only the widths that change with the viewport live here; the columns
// themselves are described where the table is rendered.
export const WORKSPACE_WIDTH_PROPERTY = '--mcp-workspace-width'

export const CLIENT_WIDTH_PROPERTY = '--mcp-client-width'

export const responsiveWidths = style({
  vars: {
    [CLIENT_WIDTH_PROPERTY]: '180px',
    [WORKSPACE_WIDTH_PROPERTY]: '260px',
  },
  '@media': {
    [MOBILE_QUERY]: {
      vars: {
        [CLIENT_WIDTH_PROPERTY]: '140px',
        [WORKSPACE_WIDTH_PROPERTY]: '180px',
      },
    },
  },
})

export const workspaceTrigger = style({
  justifyContent: 'space-between',
})

export const workspaceMenu = style({
  maxWidth: 'calc(100vw - 32px)',
  width: '228px',
})

export const installCommand = style({
  alignItems: 'center',
  display: 'flex',
  gap: '12px',
  justifyContent: 'space-between',
  minWidth: 0,
})

export const installCommandContainer = style({
  display: 'grid',
  gap: '4px',
  minWidth: 0,
})

export const copyButton = style({
  opacity: 0,
  transition: 'opacity 120ms ease',
  selectors: {
    [`${installCommand}:focus-within &`]: { opacity: 1 },
    [`${installCommand}:hover &`]: { opacity: 1 },
  },
})

globalStyle(`${installCommand} code`, {
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
