import { keyframes, style } from '@vanilla-extract/css'

import { theme, zIndex } from '@/wax/theme/theme.css'

const progress = keyframes({
  '0%': { inlineSize: '0%' },
  '5%': { inlineSize: '30%' },
  '100%': { inlineSize: '85%' },
})

export const bar = style({
  '::after': {
    animation: `${progress} 8s cubic-bezier(0.1, 0.4, 0.2, 1) forwards`,
    backgroundColor: theme.content.accentContent.primary,
    blockSize: '100%',
    content: '""',
    display: 'block',
  },
  blockSize: 2,
  inlineSize: '100%',
  insetBlockStart: 0,
  insetInlineStart: 0,
  pointerEvents: 'none',
  position: 'fixed',

  zIndex: zIndex.notification,
})
