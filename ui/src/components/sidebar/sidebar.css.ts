import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme'
import { theme, zIndex } from '@/wax/theme/theme.css'

const MAIN_CONTENT_PADDING = 12
const SIDEBAR_COLLAPSED_WIDTH = 34

export const sidebar = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      minWidth: `${SIDEBAR_COLLAPSED_WIDTH + MAIN_CONTENT_PADDING * 2}px`,
    },
  },
  backgroundColor: theme.surface.main,
  display: 'flex',
  flexDirection: 'column',
  flexShrink: 0,
  height: '100dvh',
  minWidth: '180px',
  padding: MAIN_CONTENT_PADDING,
  zIndex: zIndex.navigation,
})

export const sidebarMinimized = style({
  minWidth: `${SIDEBAR_COLLAPSED_WIDTH + MAIN_CONTENT_PADDING * 2}px`,
})

export const header = style({
  alignItems: 'center',
  display: 'flex',
  gap: '4px',
  paddingBlockStart: '10px',
  selectors: {
    [`${sidebarMinimized} &`]: {
      flexDirection: 'column',
    },
  },
})

export const brandButton = style({
  alignItems: 'center',
  background: 'transparent',
  border: 'none',
  borderRadius: '8px',
  cursor: 'default',
  display: 'flex',
  flex: 1,
  flexShrink: 0,
  gap: '8px',
  minHeight: '32px',
  overflow: 'hidden',
  paddingInline: '8px',
  selectors: {
    [`${sidebarMinimized} &`]: {
      justifyContent: 'center',
      paddingInline: 0,
    },
  },
})

export const brandName = style({
  fontFamily: 'Gustan, sans-serif',
  fontWeight: 500,
  fontSize: 15,
  lineHeight: '145%',
  color: theme.content.primary,
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
  gap: '4px',
  paddingBlockStart: '8px',
})
