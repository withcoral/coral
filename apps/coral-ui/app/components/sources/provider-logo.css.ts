import { style, styleVariants } from '@vanilla-extract/css'

import { darkTheme } from '@/wax/theme/theme-dark.css'
import { theme } from '@/wax/theme/theme.css'

export const root = style({
  alignItems: 'center',
  background: theme.surface.onMainContent,
  borderRadius: '50%',
  display: 'inline-flex',
  flexShrink: 0,
  justifyContent: 'center',
  overflow: 'hidden',
})

export const size = styleVariants({
  small: {
    height: 20,
    width: 20,
  },
  medium: {
    height: 28,
    width: 28,
  },
  large: {
    height: 40,
    width: 40,
  },
})

export const image = style({
  display: 'block',
  height: '100%',
  objectFit: 'contain',
  width: '100%',
})

export const imageInvertInDark = style({
  selectors: {
    [`${darkTheme} &`]: {
      filter: 'invert(1)',
    },
  },
})
