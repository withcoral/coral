import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { theme, zIndex } from '@/wax/theme/theme.css'

const FADE_WIDTH_PX = 40

export const listRoot = style({
  maxWidth: '100%',
  minWidth: 0,
  overflow: 'hidden',
  position: 'relative',
})

export const listViewport = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      scrollPaddingInline: '16px',
    },
  },
  maskImage: `linear-gradient(
    to right,
    transparent 0,
    black min(${FADE_WIDTH_PX}px, var(--scroll-area-overflow-x-start)),
    black calc(100% - min(${FADE_WIDTH_PX}px, var(--scroll-area-overflow-x-end, ${FADE_WIDTH_PX}px))),
    transparent 100%
  )`,
  maskRepeat: 'no-repeat',
  overscrollBehavior: 'contain',
  scrollPaddingInline: '32px',
  width: '100%',
})

export const list = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      paddingInline: '16px',
    },
  },
  boxShadow: `inset 0 -1px ${theme.stroke.primary}`,
  display: 'flex',
  gap: '4px',
  paddingBlockEnd: '8px',
  paddingInline: '32px',
  position: 'relative',
  zIndex: zIndex.base,
})

export const tab = style({
  color: theme.content.secondary,
  paddingBlock: '6px',
  paddingInline: '10px',

  selectors: {
    '&:hover': {
      color: theme.content.primary,
    },
    '&[data-disabled]': {
      color: theme.content.disabled,
    },
  },
  userSelect: 'none',
  ...theme.typography.buttonStrong,
})

export const indicator = style({
  backgroundColor: theme.content.primary,
  bottom: '0px',
  height: '1px',
  left: '0px',
  position: 'absolute',
  transitionDuration: '200ms',
  transitionProperty: 'translate, width',
  transitionTimingFunction: 'ease-in-out',
  translate: 'var(--active-tab-left)',
  width: 'var(--active-tab-width)',
})
