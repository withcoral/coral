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

// The pill contract carries a hover pair for every colour. Read both from the
// same token group so a status cannot mix one colour with another's hover.
function pillHover(pill: { backgroundHover: string; colorHover: string }) {
  return {
    selectors: {
      '&:hover:not(:disabled)': {
        background: pill.backgroundHover,
        color: pill.colorHover,
      },
    },
  }
}

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
  compoundVariants: [
    {
      style: pillHover(theme.pill.blue),
      variants: { isInteractive: true, status: 'available' },
    },
    {
      style: pillHover(theme.pill.green),
      variants: { isInteractive: true, status: 'ready' },
    },
  ],
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
