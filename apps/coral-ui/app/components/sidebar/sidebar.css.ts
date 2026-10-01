import { style } from '@vanilla-extract/css'
import { recipe } from '@vanilla-extract/recipes'

import { breakpoints } from '@/styles/theme.css'
import { animation, theme, zIndex } from '@/wax/theme/theme.css'

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

export const workspaceSelectorRow = style({
  alignItems: 'center',
  display: 'flex',
  gap: '4px',
  minHeight: '32px',
  minWidth: 0,
})

export const workspaceSelector = style({
  flex: '1 1 auto',
  justifyContent: 'flex-start',
  minWidth: 0,
  overflow: 'hidden',
})

export const workspaceSelectorLabel = style({
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

export const workspaceSelectorChevron = style({
  display: 'flex',
  flexShrink: 0,
  marginInlineStart: 'auto',
})

export const workspaceSelectorMark = recipe({
  base: {
    alignItems: 'center',
    borderRadius: '8px',
    display: 'flex',
    flexShrink: 0,
    height: '20px',
    justifyContent: 'center',
    transition: animation.colorTransition,
    width: '20px',
  },
  variants: {
    color: {
      ...theme.avatarFallback,
    },
  },
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

export const workspaceMenuForm = style({
  display: 'contents',
})

export const nav = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: '4px',
  marginBlockStart: '24px',
  minHeight: 0,
})
