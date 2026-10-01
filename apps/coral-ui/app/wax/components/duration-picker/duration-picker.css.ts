import { keyframes, style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

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

export const popup = style({
  backgroundColor: theme.surface.floating,
  border: `1px solid ${theme.stroke.floating}`,
  borderRadius: '10px',
  boxShadow: theme.elevation.e3,
  outline: 'none',
  padding: '4px',
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

export const fields = style({
  alignItems: 'center',
  display: 'flex',
  gap: '8px',
  paddingBlock: '8px 6px',
  paddingInline: '8px',
})

export const label = style({
  paddingBlockEnd: '8px',
  paddingInline: '8px 2px',
})

export const input = style({
  width: '38px',
})
