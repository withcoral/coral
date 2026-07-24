import { style, styleVariants } from '@vanilla-extract/css'

import { theme, zIndex } from '@/wax/theme/theme.css'

const FADE_SIZE_PX = 40

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
      height: `min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-y-start))`,
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
      height: `min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-y-end, ${FADE_SIZE_PX}px))`,
      vars: {
        '--scroll-area-overflow-y-end': 'inherit',
      },
    },
  },
})

const horizontalFadeBase = style({
  selectors: {
    '&::before, &::after': {
      content: '""',
      display: 'block',
      height: '100%',
      pointerEvents: 'none',
      position: 'absolute',
      top: 0,
      transition: 'width 0.1s ease-out',
      zIndex: zIndex.raised,
    },
  },
})

const fadeLeft = style({
  selectors: {
    '&::before': {
      background: `linear-gradient(to right, ${fadeColor}, transparent)`,
      left: 0,
      vars: {
        '--scroll-area-overflow-x-start': 'inherit',
      },
      width: `min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-x-start))`,
    },
  },
})

const fadeRight = style({
  selectors: {
    '&::after': {
      background: `linear-gradient(to left, ${fadeColor}, transparent)`,
      right: 0,
      vars: {
        '--scroll-area-overflow-x-end': 'inherit',
      },
      width: `min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-x-end, ${FADE_SIZE_PX}px))`,
    },
  },
})

export const viewportFade = styleVariants({
  both: [fadeBase, fadeTop, fadeBottom],
  bottom: [fadeBase, fadeBottom],
  horizontal: [horizontalFadeBase, fadeLeft, fadeRight],
  none: [],
  top: [fadeBase, fadeTop],
})

const nativeFadeTop = style({
  maskImage: `linear-gradient(
    to bottom,
    transparent 0,
    black min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-y-start)),
    black 100%
  )`,
  maskRepeat: 'no-repeat',
  scrollPaddingBlockStart: FADE_SIZE_PX,
})

const nativeFadeBottom = style({
  maskImage: `linear-gradient(
    to bottom,
    black 0,
    black calc(100% - min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-y-end, ${FADE_SIZE_PX}px))),
    transparent 100%
  )`,
  maskRepeat: 'no-repeat',
  scrollPaddingBlockEnd: FADE_SIZE_PX,
})

const nativeFadeBoth = style({
  maskImage: `linear-gradient(
    to bottom,
    transparent 0,
    black min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-y-start)),
    black calc(100% - min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-y-end, ${FADE_SIZE_PX}px))),
    transparent 100%
  )`,
  maskRepeat: 'no-repeat',
  scrollPaddingBlock: FADE_SIZE_PX,
})

const nativeFadeHorizontal = style({
  maskImage: `linear-gradient(
    to right,
    transparent 0,
    black min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-x-start)),
    black calc(100% - min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-x-end, ${FADE_SIZE_PX}px))),
    transparent 100%
  )`,
  maskRepeat: 'no-repeat',
  scrollPaddingInline: FADE_SIZE_PX,
})

export const nativeViewportFade = styleVariants({
  both: [nativeFadeBoth],
  bottom: [nativeFadeBottom],
  horizontal: [nativeFadeHorizontal],
  none: {},
  top: [nativeFadeTop],
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
