import { style } from '@vanilla-extract/css'

import { theme, zIndex } from '@/wax/theme/theme.css'

const MAIN_CONTENT_PADDING = 12
const NAVBAR_COLLAPSED_WIDTH = 34

export const navbar = style({
  backgroundColor: theme.surface.main,
  display: 'flex',
  flexDirection: 'column',
  flexShrink: 0,
  height: '100dvh',
  minWidth: `${NAVBAR_COLLAPSED_WIDTH + MAIN_CONTENT_PADDING * 2}px`,
  padding: MAIN_CONTENT_PADDING,
  zIndex: zIndex.navigation,
})

export const header = style({
  alignItems: 'center',
  display: 'flex',
  justifyContent: 'center',
  minHeight: '32px',
  paddingBlockStart: '10px',
})

export const brandButton = style({
  alignItems: 'center',
  background: 'transparent',
  border: 'none',
  borderRadius: '8px',
  display: 'flex',
  height: '32px',
  justifyContent: 'center',
  padding: 0,
  width: '32px',
})

export const nav = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: '4px',
  marginBlockStart: '24px',
})

export const navButton = style({
  alignItems: 'center',
  background: 'transparent',
  border: 'none',
  borderRadius: '8px',
  color: theme.content.tertiary,
  cursor: 'default',
  display: 'flex',
  height: '32px',
  justifyContent: 'center',
  padding: 0,
  width: '34px',
  selectors: {
    '&[data-active="true"]': {
      background: theme.sidebar.button.selected,
      color: theme.content.primary,
    },
  },
})
