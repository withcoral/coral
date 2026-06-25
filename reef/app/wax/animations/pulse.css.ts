import { keyframes } from '@vanilla-extract/css'

export const pulseKeyframes = keyframes({
  '0%, 100%': {
    opacity: 1,
  },
  '50%': {
    opacity: 0.5,
  },
})
export const pulse = {
  animation: `${pulseKeyframes} 2s cubic-bezier(0.4, 0, 0.6, 1) infinite`,
}
