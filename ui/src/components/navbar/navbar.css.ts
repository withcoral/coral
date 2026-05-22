import { style } from '@vanilla-extract/css'
import { recipe } from '@vanilla-extract/recipes'

import { theme, zIndex } from '@/wax/theme/theme.css'

const MAIN_CONTENT_PADDING = 12
const NAVBAR_COLLAPSED_WIDTH = 58
const NAVBAR_EXPANDED_WIDTH = 240

export const navbar = recipe({
  base: {
    backgroundColor: theme.surface.main,
    display: 'flex',
    flexDirection: 'column',
    flexShrink: 0,
    height: '100dvh',
    overflow: 'hidden',
    padding: MAIN_CONTENT_PADDING,
    transition: 'width 160ms ease',
    zIndex: zIndex.navigation,
  },
  defaultVariants: {
    isCollapsed: false,
  },
  variants: {
    isCollapsed: {
      false: {
        width: `${NAVBAR_EXPANDED_WIDTH}px`,
      },
      true: {
        width: `${NAVBAR_COLLAPSED_WIDTH}px`,
      },
    },
  },
})

export const header = recipe({
  base: {
    alignItems: 'center',
    display: 'flex',
    gap: '8px',
    justifyContent: 'flex-start',
    minHeight: '32px',
    paddingBlockStart: '10px',
    width: '100%',
  },
  defaultVariants: {
    isCollapsed: false,
  },
  variants: {
    isCollapsed: {
      false: {
        flexDirection: 'row',
      },
      true: {
        flexDirection: 'column',
        alignItems: 'flex-start',
      },
    },
  },
})

export const brandMark = style({
  alignItems: 'center',
  background: 'transparent',
  border: 'none',
  borderRadius: '8px',
  color: theme.content.primary,
  display: 'flex',
  height: '32px',
  justifyContent: 'center',
  padding: 0,
  width: '32px',
})

export const nav = style({
  alignItems: 'stretch',
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: '4px',
  marginBlockStart: '24px',
})
