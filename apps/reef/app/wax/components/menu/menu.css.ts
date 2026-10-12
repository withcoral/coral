import { keyframes, style } from '@/wax/css'

import { animation, theme } from '@/wax/theme/theme.css'
import { zIndex } from '@/wax/theme/z-index'

const fadeIn = keyframes({
  from: {
    opacity: 0,
    transform: 'scale(0.95)',
  },
  to: {
    opacity: 1,
    transform: 'scale(1)',
  },
})

const fadeOut = keyframes({
  from: {
    opacity: 1,
    transform: 'scale(1)',
  },
  to: {
    opacity: 0,
    transform: 'scale(0.95)',
  },
})

export const trigger = style({
  selectors: {
    '&[data-popup-open]': {
      // Style when menu is open, if needed
    },
  },
})

export const positioner = style({
  zIndex: zIndex.tooltip,
})

export const POPUP_PADDING_PX = 4
export const popup = style({
  backgroundColor: theme.surface.floating,
  border: `1px solid ${theme.stroke.floating}`,
  borderRadius: '10px',
  boxShadow: theme.elevation.e3,
  display: 'flex',
  flexDirection: 'column',
  gap: '2px',
  maxHeight: '400px',
  outline: 'none',
  overflowX: 'hidden',
  overflowY: 'auto',
  padding: `${POPUP_PADDING_PX}px`,
  selectors: {
    '&[data-closed]': {
      animation: `${fadeOut} 0.1s ease-in`,
    },
    '&[data-open]': {
      animation: `${fadeIn} 0.15s ease-out`,
    },
  },
  transformOrigin: 'var(--transform-origin)',
  width: '180px',
})

export const item = style({
  alignItems: 'center',
  borderRadius: '8px',
  color: theme.content.primary,
  cursor: 'pointer',
  display: 'flex',
  gap: '10px',
  height: '32px',
  outline: 'none',
  overflow: 'clip',
  paddingBlock: '6px',
  paddingInlineEnd: '2px',
  paddingInlineStart: '8px',
  textDecoration: 'none',
  transition: animation.colorTransition,
  width: '100%',
  ...theme.typography.body,
  selectors: {
    '&[data-disabled]': {
      color: theme.content.disabled,
      cursor: 'not-allowed',
    },
    '&[data-highlighted]': {
      backgroundColor: theme.surface.onMainContent,
    },
  },
})

export const itemLoading = style([
  item,
  {
    cursor: 'default',
  },
])

export const itemContent = style({
  alignItems: 'center',
  display: 'flex',
  flex: '1 0 0',
  gap: '4px',
  overflow: 'hidden',
})

export const itemLabel = style({
  flex: '1 0 0',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const separator = style({
  backgroundColor: theme.stroke.primary,
  height: '1px',
  marginBlock: '4px',
  marginInline: '8px',
})

export const separatorFullWidth = style({
  marginInline: `-${POPUP_PADDING_PX}px`,
  width: `calc(100% + ${POPUP_PADDING_PX * 2}px)`,
})

export const groupLabel = style({
  paddingBlock: '6px',
  paddingInline: '8px',
})

export const submenuTrigger = style({
  paddingInlineEnd: '4px',
})

export const submenuArrow = style({
  color: theme.content.tertiary,
  display: 'flex',
  flexShrink: 0,
  marginInlineStart: 'auto',
})

export const radioIndicator = style({
  display: 'flex',
  flexShrink: 0,
  marginInlineEnd: '4px',
  marginInlineStart: 'auto',
})
