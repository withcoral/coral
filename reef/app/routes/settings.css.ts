import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { theme } from '@/wax/theme/theme.css'

const MOBILE_QUERY = `screen and (max-width: ${breakpoints.mobile})`

export const page = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '28px',
  height: '100%',
  overflow: 'auto',
  paddingBlock: '28px',
  paddingInline: '32px',
  '@media': {
    [MOBILE_QUERY]: {
      gap: '22px',
      paddingBlock: '20px',
      paddingInline: '16px',
    },
  },
})

export const header = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '6px',
  maxWidth: '680px',
})

export const section = style({
  borderBlockStart: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexDirection: 'column',
  gap: '16px',
  paddingBlockStart: '20px',
})

export const sectionHeader = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '4px',
  maxWidth: '720px',
})

export const commandRow = style({
  alignItems: 'center',
  display: 'grid',
  gap: '16px',
  gridTemplateColumns: 'minmax(0, 1fr) auto',
  maxWidth: '960px',
  '@media': {
    [MOBILE_QUERY]: {
      alignItems: 'stretch',
      gridTemplateColumns: '1fr',
    },
  },
})

export const clientList = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
  maxWidth: '960px',
})

export const clientRow = style({
  alignItems: 'center',
  background: theme.surface.onMainContentSubtle,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: '8px',
  display: 'grid',
  gap: '16px',
  gridTemplateColumns: 'minmax(0, 1fr) auto',
  minHeight: '68px',
  paddingBlock: '12px',
  paddingInline: '14px',
  '@media': {
    [MOBILE_QUERY]: {
      alignItems: 'stretch',
      gridTemplateColumns: '1fr',
    },
  },
})

export const rowContent = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '4px',
  minWidth: 0,
})

export const rowActions = style({
  alignItems: 'center',
  display: 'flex',
  gap: '8px',
  justifyContent: 'flex-end',
  '@media': {
    [MOBILE_QUERY]: {
      justifyContent: 'flex-start',
    },
  },
})

export const path = style({
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const status = style({
  minHeight: '18px',
})
