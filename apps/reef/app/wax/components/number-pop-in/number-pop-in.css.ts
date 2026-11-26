import { keyframes, style } from '@/wax/css'

// Number pop-in (transitions.dev): each character rises into place as it fades in
// and sharpens, with a slight overshoot, one after another.
const popIn = keyframes({
  from: { filter: 'blur(2px)', opacity: 0, transform: 'translateY(8px)' },
})

export const group = style({
  alignItems: 'baseline',
  display: 'inline-flex',
  whiteSpace: 'pre',
})

export const character = style({
  animation: `${popIn} 500ms cubic-bezier(0.34, 1.45, 0.64, 1) both`,
  display: 'inline-block',
  '@media': { '(prefers-reduced-motion: reduce)': { animation: 'none' } },
})
