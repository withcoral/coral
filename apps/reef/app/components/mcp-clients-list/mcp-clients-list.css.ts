import { globalStyle, style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { theme } from '@/wax/theme/theme.css'

const MOBILE_QUERY = `screen and (max-width: ${breakpoints.mobile})`

export const statusCell = style({
  paddingBlock: '24px',
  paddingInline: '12px',
  textAlign: 'center',
})

export const tableContainer = style({
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: '10px',
  overflow: 'hidden',
})

export const table = style({
  tableLayout: 'fixed',
})

export const workspaceColumn = style({
  width: '260px',
  '@media': {
    [MOBILE_QUERY]: {
      width: '180px',
    },
  },
})

export const clientColumn = style({
  width: '180px',
  '@media': {
    [MOBILE_QUERY]: {
      width: '140px',
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

export const installColumn = style({
  width: 'auto',
})

export const installCommand = style({
  alignItems: 'center',
  display: 'flex',
  gap: '12px',
  justifyContent: 'space-between',
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

// Table cells normally truncate to preserve dense configuration rows. Manual
// remote-client setup needs the endpoint and every step to remain readable.
export const setupCell = style({
  maxWidth: 'none',
  overflow: 'visible',
  overflowWrap: 'anywhere',
  textOverflow: 'clip',
  whiteSpace: 'normal',
})

globalStyle(`${installCommand} code`, {
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
