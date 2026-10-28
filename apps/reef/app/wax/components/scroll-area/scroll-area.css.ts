import { style, styleVariants } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

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
  selectors: {
    // Not a tab stop: only a click focuses it, and a later key press must not
    // outline the whole area as if the keyboard had.
    '&[tabindex="-1"]:focus-visible': { outline: 'none' },
  },
})

/** The reading controller, rather than browser heuristics, owns record anchoring. */
export const anchoredViewport = style({ overflowAnchor: 'none' })

// A mask fades the viewport itself, so the fade matches any background behind it.
const fadeTop = style({
  maskImage: `linear-gradient(
    to bottom,
    transparent 0,
    black min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-y-start)),
    black 100%
  )`,
  maskRepeat: 'no-repeat',
})

const fadeBottom = style({
  maskImage: `linear-gradient(
    to bottom,
    black 0,
    black calc(100% - min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-y-end, ${FADE_SIZE_PX}px))),
    transparent 100%
  )`,
  maskRepeat: 'no-repeat',
})

const fadeBoth = style({
  maskImage: `linear-gradient(
    to bottom,
    transparent 0,
    black min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-y-start)),
    black calc(100% - min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-y-end, ${FADE_SIZE_PX}px))),
    transparent 100%
  )`,
  maskRepeat: 'no-repeat',
})

const fadeHorizontal = style({
  maskImage: `linear-gradient(
    to right,
    transparent 0,
    black min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-x-start)),
    black calc(100% - min(${FADE_SIZE_PX}px, var(--scroll-area-overflow-x-end, ${FADE_SIZE_PX}px))),
    transparent 100%
  )`,
  maskRepeat: 'no-repeat',
})

export const viewportFade = styleVariants({
  both: [fadeBoth],
  bottom: [fadeBottom],
  horizontal: [fadeHorizontal],
  none: {},
  top: [fadeTop],
})

/** A native viewport, such as a textarea, keeps the caret clear of its fades. */
export const nativeViewportFade = styleVariants({
  both: { scrollPaddingBlock: FADE_SIZE_PX },
  bottom: { scrollPaddingBlockEnd: FADE_SIZE_PX },
  horizontal: { scrollPaddingInline: FADE_SIZE_PX },
  none: {},
  top: { scrollPaddingBlockStart: FADE_SIZE_PX },
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
      marginInlineEnd: '4px',
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
