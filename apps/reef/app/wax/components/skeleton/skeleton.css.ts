import { keyframes, style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

const shimmer = keyframes({
  '0%': {
    backgroundPosition: '100% 0',
  },
  '100%': {
    backgroundPosition: '-100% 0',
  },
})

export const container = style({
  animation: `${shimmer} 1.5s ease 0s infinite normal forwards`,
  background: theme.surface.skeleton,
  backgroundImage: `linear-gradient(
    90deg,
    transparent 0%,
    ${theme.surface.skeleton} 20%,
    transparent 40%,
    transparent 100%
  )`,
  backgroundPosition: '100% 0',
  backgroundRepeat: 'no-repeat',
  backgroundSize: '200% 100%',
  borderRadius: '4px',
  display: 'block',
})

// One line box of the text it stands for, so the text that replaces it takes no more room.
// Inline, so it follows the text alignment of the line it stands in.
export const textLine = style({
  display: 'inline-flex',
  verticalAlign: 'top',
  alignItems: 'center',
  height: '1lh',
  maxWidth: '100%',
})
export const textBar = style({ height: '1em', width: '100%' })
