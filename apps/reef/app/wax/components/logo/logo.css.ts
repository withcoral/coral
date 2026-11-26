import { recipe, style } from '@/wax/css'

import { darkTheme } from '@/wax/theme/theme-dark.css'
import { lightTheme } from '@/wax/theme/theme-light.css'
import { theme } from '@/wax/theme/theme.css'

// A rounded square plate, so every logo has the same shape and reads on any surface.
export const logoContainer = recipe({
  base: {
    alignItems: 'center',
    background: theme.logoPlate.background,
    display: 'inline-flex',
    flex: '0 0 auto',
    justifyContent: 'center',
    overflow: 'hidden',
    position: 'relative',
    userSelect: 'none',
    verticalAlign: 'top',
  },

  defaultVariants: {
    size: '32',
  },

  variants: {
    size: {
      // A button's icon size at Button size 32, so a logo can lead its label.
      '18': { borderRadius: '4px', height: '18px', width: '18px' },
      '20': { borderRadius: '5px', height: '20px', width: '20px' },
      '32': { borderRadius: '8px', height: '32px', width: '32px' },
      '40': { borderRadius: '10px', height: '40px', width: '40px' },
    },
  },
})

export const logoBorderOverlay = style({
  border: `1px solid ${theme.logoPlate.stroke}`,
  borderRadius: 'inherit',
  display: 'block',
  inset: 0,
  pointerEvents: 'none',
  position: 'absolute',
})

// Inset from the edge, so a logo that fills its own square keeps a margin on the plate.
export const logoImage = style({
  boxSizing: 'border-box',
  height: '100%',
  objectFit: 'contain',
  padding: '16%',
  width: '100%',
})

// Solid colours, so the letter does not blend into the plate behind it.
export const logoFallback = recipe({
  base: {
    alignItems: 'center',
    display: 'flex',
    height: '100%',
    justifyContent: 'center',
    width: '100%',
    ...theme.typography.bodySmallStrong,
  },

  defaultVariants: {
    size: '32',
  },

  variants: {
    color: {
      ...theme.avatarFallback,
    },
    size: {
      '18': { fontSize: 10 },
      '20': { fontSize: 11 },
      '32': {},
      '40': {},
    },
  },
})

// A logo with a dark-theme image renders twice; the nearest theme class shows
// one. The second selector restores the light image in a light panel nested
// inside a dark one.
export const lightOnly = style({
  selectors: {
    [`${darkTheme} &`]: { display: 'none' },
    [`${darkTheme} ${lightTheme} &`]: { display: 'inline-flex' },
  },
})

export const darkOnly = style({
  display: 'none',
  selectors: {
    [`${darkTheme} &`]: { display: 'inline-flex' },
    [`${darkTheme} ${lightTheme} &`]: { display: 'none' },
  },
})
