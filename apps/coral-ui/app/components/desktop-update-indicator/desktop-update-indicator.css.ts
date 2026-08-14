import { style } from '@vanilla-extract/css'
import { recipe } from '@vanilla-extract/recipes'

import { breakpoints } from '@/styles/theme.css'
import { animation, theme } from '@/wax/theme/theme.css'

const MOBILE_QUERY = `screen and (max-width: ${breakpoints.mobile})`

const collapsedGeometry = {
  gap: 0,
  justifyContent: 'center',
  minHeight: '34px',
  padding: 0,
  width: '34px',
} as const

export const indicator = recipe({
  base: {
    '@media': {
      [MOBILE_QUERY]: collapsedGeometry,
    },
    alignItems: 'center',
    border: '1px solid',
    borderRadius: '8px',
    boxSizing: 'border-box',
    display: 'flex',
    flexShrink: 0,
    gap: '8px',
    marginBlockStart: '12px',
    minHeight: '48px',
    minWidth: 0,
    overflow: 'hidden',
    paddingBlock: '7px',
    paddingInline: '8px',
    transition: animation.colorTransition,
    width: '100%',
  },
  defaultVariants: {
    isMinimized: false,
    status: 'available',
  },
  variants: {
    isMinimized: {
      false: {},
      true: collapsedGeometry,
    },
    status: {
      available: {
        background: theme.pill.blue.background,
        borderColor: theme.pill.blue.stroke,
        color: theme.pill.blue.color,
      },
      downloading: {
        background: theme.pill.blue.background,
        borderColor: theme.pill.blue.stroke,
        color: theme.pill.blue.color,
      },
      ready: {
        background: theme.pill.green.background,
        borderColor: theme.pill.green.stroke,
        color: theme.pill.green.color,
      },
    },
  },
})

export const icon = style({
  flexShrink: 0,
})

export const copy = style({
  '@media': {
    [MOBILE_QUERY]: {
      display: 'none',
    },
  },
  display: 'flex',
  flexDirection: 'column',
  minWidth: 0,
})
