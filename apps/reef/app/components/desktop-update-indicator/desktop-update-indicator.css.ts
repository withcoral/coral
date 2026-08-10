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
    // The button variant would otherwise center its two unequal copy lines.
    textAlign: 'start',
    transition: animation.colorTransition,
    width: '100%',
  },
  defaultVariants: {
    isInteractive: false,
    isMinimized: false,
    status: 'available',
  },
  variants: {
    isInteractive: {
      false: {},
      true: {
        cursor: 'pointer',
        selectors: {
          '&:disabled': {
            cursor: 'default',
            opacity: 0.6,
          },
          '&:focus-visible': {
            outline: `1px solid ${theme.button.primary.focus}`,
          },
        },
      },
    },
    isMinimized: {
      false: {},
      true: collapsedGeometry,
    },
    status: {
      available: {
        background: theme.pill.blue.background,
        borderColor: theme.pill.blue.stroke,
        color: theme.pill.blue.color,
        selectors: {
          // Scoped to the tag rather than `isInteractive` so it cannot drift:
          // `downloading` renders a plain div, which has no `:disabled` to fail
          // against and would otherwise hover like a button.
          'button&:hover:not(:disabled)': {
            background: theme.pill.blue.backgroundHover,
            color: theme.pill.blue.colorHover,
          },
        },
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
        selectors: {
          'button&:hover:not(:disabled)': {
            background: theme.pill.green.backgroundHover,
            color: theme.pill.green.colorHover,
          },
        },
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
