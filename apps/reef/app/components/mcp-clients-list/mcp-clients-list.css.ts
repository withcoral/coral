import { style } from '@vanilla-extract/css'

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

export const workspaceTrigger = style({
  justifyContent: 'space-between',
})

export const workspaceMenu = style({
  maxWidth: 'calc(100vw - 32px)',
  width: '228px',
})
