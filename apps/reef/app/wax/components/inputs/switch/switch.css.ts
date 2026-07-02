import { style } from '@vanilla-extract/css'

import { animation, staticUtilities, theme } from '@/wax/theme/theme.css'

export const root = style({
  alignItems: 'center',
  backgroundColor: theme.switch.default,
  border: 'none',
  borderRadius: '100px',
  cursor: 'pointer',
  display: 'flex',
  height: '22px',
  outline: 'none',
  padding: '2px',
  selectors: {
    '&:hover:not([data-disabled])': {
      backgroundColor: theme.switch.hover,
    },
    '&[data-checked]': {
      backgroundColor: theme.switch.on,
    },
    '&[data-checked]:hover:not([data-disabled])': {
      backgroundColor: theme.switch.hoverOn,
    },
    '&[data-disabled]': {
      cursor: 'not-allowed',
      opacity: 0.5,
    },
  },
  transition: animation.colorTransition,

  width: '36px',
})

export const thumb = style({
  backgroundColor: staticUtilities.white,
  borderRadius: '100px',
  height: '18px',
  selectors: {
    '[data-checked] &': {
      transform: 'translateX(14px)',
    },
  },
  transition: 'transform 0.2s ease',

  width: '18px',
})
