import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { theme, zIndex } from '@/wax/theme/theme.css'

const MAIN_CONTENT_PADDING = 12
const SIDEBAR_COLLAPSED_WIDTH = 34
const SIDEBAR_COLLAPSED_BASIS = `${SIDEBAR_COLLAPSED_WIDTH + MAIN_CONTENT_PADDING * 2}px`
const SIDEBAR_EXPANDED_WIDTH = 180

export const sidebar = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      flexBasis: SIDEBAR_COLLAPSED_BASIS,
      minWidth: SIDEBAR_COLLAPSED_BASIS,
    },
  },
  backgroundColor: theme.surface.main,
  display: 'flex',
  flexDirection: 'column',
  flex: `0 0 ${SIDEBAR_EXPANDED_WIDTH}px`,
  height: '100dvh',
  minWidth: 0,
  padding: MAIN_CONTENT_PADDING,
  zIndex: zIndex.navigation,
})

export const sidebarMinimized = style({
  flexBasis: SIDEBAR_COLLAPSED_BASIS,
})

export const header = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '4px',
  paddingBlockStart: '10px',
})

export const brandRow = style({
  alignItems: 'center',
  display: 'flex',
  gap: '4px',
  minWidth: 0,
})

export const brandMark = style({
  alignItems: 'center',
  borderRadius: '8px',
  backgroundColor: theme.surface.main,
  color: theme.content.primary,
  display: 'flex',
  flexShrink: 0,
  height: '32px',
  justifyContent: 'center',
  width: '32px',
})

export const brandLabel = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      display: 'none',
    },
  },
  flex: '1 1 auto',
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const toggleButton = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      display: 'none',
    },
  },
  display: 'flex',
  justifyContent: 'center',
})

export const nav = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: '4px',
  marginBlockStart: '24px',
  minHeight: 0,
})

export const footer = style({
  display: 'flex',
  flexDirection: 'column',
  flexShrink: 0,
  gap: '4px',
  marginTop: 'auto',
})
