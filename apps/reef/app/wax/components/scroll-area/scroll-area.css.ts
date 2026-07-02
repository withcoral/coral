import { style, styleVariants } from '@vanilla-extract/css'

import { theme, zIndex } from '@/wax/theme/theme.css'

const FADE_HEIGHT_PX = 40

export const root = style({
  overflow: 'hidden',
  position: 'relative',
})

export const viewport = style({
  height: '100%',
  maxHeight: 'inherit',
  overscrollBehavior: 'contain',
  width: '100%',
})

const fadeBase = style({
  selectors: {
    '&::before, &::after': {
      content: '""',
      display: 'block',
      left: 0,
      pointerEvents: 'none',
      position: 'absolute',
      transition: 'height 0.1s ease-out',
      width: '100%',
      zIndex: zIndex.raised,
    },
  },
})

const fadeColor = theme.surface.mainContent

const fadeTop = style({
  selectors: {
    '&::before': {
      background: `linear-gradient(to bottom, ${fadeColor}, transparent)`,
      height: `min(${FADE_HEIGHT_PX}px, var(--scroll-area-overflow-y-start))`,
      top: 0,
      vars: {
        '--scroll-area-overflow-y-start': 'inherit',
      },
    },
  },
})

const fadeBottom = style({
  selectors: {
    '&::after': {
      background: `linear-gradient(to top, ${fadeColor}, transparent)`,
      bottom: 0,
      height: `min(${FADE_HEIGHT_PX}px, var(--scroll-area-overflow-y-end, ${FADE_HEIGHT_PX}px))`,
      vars: {
        '--scroll-area-overflow-y-end': 'inherit',
      },
    },
  },
})

export const viewportFade = styleVariants({
  both: [fadeBase, fadeTop, fadeBottom],
  bottom: [fadeBase, fadeBottom],
  none: [],
  top: [fadeBase, fadeTop],
})

export const content = style({
  display: 'block',
})

export const scrollbar = style({
  backgroundColor: theme.surface.onMainContent,
  borderRadius: '6px',
  display: 'flex',
  margin: '8px',
  opacity: 0,
  pointerEvents: 'none',
  position: 'relative',
  selectors: {
    '&::before': {
      content: '""',
      position: 'absolute',
    },
    '&[data-hovering], &[data-scrolling]': {
      opacity: 1,
      pointerEvents: 'auto',
    },
    '&[data-orientation="horizontal"]': {
      height: '4px',
      margin: '8px',
    },
    '&[data-orientation="horizontal"]::before': {
      bottom: '-8px',
      height: '20px',
      left: 0,
      right: 0,
      width: '100%',
    },
    '&[data-orientation="vertical"]': {
      margin: '8px',
      width: '4px',
    },
    '&[data-orientation="vertical"]::before': {
      height: '100%',
      left: '50%',
      transform: 'translateX(-50%)',
      width: '20px',
    },
    '&[data-scrolling]': {
      transitionDuration: '0ms',
    },
  },
  touchAction: 'none',
  transition: 'opacity 150ms',
  userSelect: 'none',
})

export const thumb = style({
  backgroundColor: theme.surface.onMainContentHover,
  borderRadius: 'inherit',
  width: '100%',
})

export const corner = style({
  height: '4px',
  width: '4px',
})
