import { style } from '@vanilla-extract/css'

import { input } from '@/wax/components/inputs/base-input.css'
import { animation, theme } from '@/wax/theme/theme.css'

export const textArea = style([
  input,
  {
    border: 0,
    borderRadius: 'inherit',
    display: 'block',
    minHeight: 72,
    resize: 'vertical',
  },
])

export const textAreaContainer = style({
  border: `1px solid ${theme.input.stroke.default}`,
  borderRadius: 8,
  transition: animation.colorTransition,
  width: '100%',
  selectors: {
    '&:has(textarea:disabled)': {
      borderColor: theme.input.stroke.disabled,
      cursor: 'not-allowed',
    },
    '&:focus-within': {
      borderColor: theme.input.stroke.focus,
    },
    '&:hover:not(:focus-within):not(:has(textarea:disabled))': {
      borderColor: theme.input.stroke.hover,
    },
  },
})
