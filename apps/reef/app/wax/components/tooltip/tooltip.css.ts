import { keyframes, style } from '@/wax/css'

import { theme, zIndex } from '@/wax/theme/theme.css'

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

export const positioner = style({
  zIndex: zIndex.tooltip,
})

export const popup = style({
  backgroundColor: theme.surface.floating,
  border: `1px solid ${theme.stroke.primary}`,
  borderRadius: '8px',
  boxShadow: theme.elevation.e2,
  color: theme.content.secondary,
  paddingBlock: '4px',
  paddingInline: '8px',
  ...theme.typography.bodySmall,
  // A URL or identifier has no spaces; break it rather than cross the edge.
  overflowWrap: 'anywhere',
  selectors: {
    '&[data-closed]': {
      animation: `${fadeOut} 0.1s ease-in`,
    },
    '&[data-open]': {
      animation: `${fadeIn} 0.15s ease-out`,
    },
  },
  transformOrigin: 'var(--transform-origin)',
})

export const arrow = style({
  fill: theme.surface.floating,
  height: '8px',
  width: '12px',
})

export const infoTipLabel = style({
  alignItems: 'center',
  display: 'inline-flex',
  gap: '6px',
})

export const infoTip = style({
  alignItems: 'center',
  background: 'none',
  border: 'none',
  borderRadius: '50%',
  cursor: 'help',
  display: 'inline-flex',
  padding: 0,
  selectors: {
    '&:focus-visible': {
      outline: `2px solid ${theme.stroke.focused}`,
      outlineOffset: 2,
    },
  },
})
