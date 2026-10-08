import { keyframes, style } from '@/wax/css'
import type { StyleRule } from '@/wax/css'

const reducedMotion = {
  '@media': {
    '(prefers-reduced-motion: reduce)': {
      animation: 'none',
    },
  },
} satisfies StyleRule

const pulse = keyframes({
  '0%, 100%': {
    opacity: 1,
  },
  '50%': {
    opacity: 0.5,
  },
})

const spin = keyframes({
  from: { transform: 'rotate(0deg)' },
  to: { transform: 'rotate(360deg)' },
})

export const pulseAnimation = style({
  animation: `${pulse} 2s cubic-bezier(0.4, 0, 0.6, 1) infinite`,
  ...reducedMotion,
})

export const spinAnimation = style({
  animation: `${spin} 1s linear infinite`,
  ...reducedMotion,
})
