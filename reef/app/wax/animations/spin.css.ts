import { keyframes, style } from '@vanilla-extract/css'

const spin = keyframes({
  from: { transform: 'rotate(0deg)' },
  to: { transform: 'rotate(360deg)' },
})

export const spinAnimation = style({
  animation: `${spin} 1s linear infinite`,
})
