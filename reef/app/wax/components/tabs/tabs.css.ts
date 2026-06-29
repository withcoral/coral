import { globalStyle, style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { theme, zIndex } from '@/wax/theme/theme.css'

export const list = style({
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      paddingInline: '16px',
      scrollPaddingInline: '16px',
    },
  },
  boxShadow: `inset 0 -1px ${theme.stroke.primary}`,
  display: 'flex',
  gap: '4px',
  overflowX: 'auto',
  paddingBlockEnd: '8px',
  paddingInline: '32px',
  position: 'relative',
  scrollbarWidth: 'none',
  scrollPaddingInline: '32px',
  zIndex: zIndex.base,
})

globalStyle(`${list}::-webkit-scrollbar`, {
  display: 'none',
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
