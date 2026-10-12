import { style } from '@/wax/css'

import { animation, theme } from '@/wax/theme/theme.css'

export const baseInput = style({
  backgroundColor: 'transparent',
  border: `1px solid ${theme.input.stroke.default}`,
  borderRadius: '8px',
  boxShadow: 'none',
  color: theme.content.primary,
  outline: 'none',
  paddingBlock: '6px',
  paddingInline: '12px',
  transition: animation.colorTransition,
  width: '100%',
  ...theme.typography.body,
})

export const input = style([
  baseInput,
  {
    selectors: {
      '&::placeholder': {
        color: theme.content.placeholder,
      },
      '&:disabled': {
        borderColor: theme.input.stroke.disabled,
        color: theme.content.disabled,
        cursor: 'not-allowed',
      },
      '&:disabled::placeholder': {
        color: theme.content.disabled,
      },
      '&:focus:not([data-invalid])': {
        borderColor: theme.input.stroke.focus,
      },
      '&:hover:not(:focus):not(:disabled):not([data-invalid])': {
        borderColor: theme.input.stroke.hover,
      },
      '&[data-invalid]:not(:disabled)': {
        borderColor: theme.input.stroke.error,
      },
    },
  },
])

// Borderless at rest, so a grid of inputs reads as a table; hover, focus and an
// error still draw the border, because those rules outrank this one.
export const bareInput = style({ borderColor: 'transparent' })

export const container = style({
  alignItems: 'center',
  display: 'flex',
  position: 'relative',
})

export const inputWithIcon = style({
  paddingInlineStart: '36px',
})

export const iconWrapper = style({
  insetBlockStart: '50%',
  insetInlineStart: '9px',
  pointerEvents: 'none',
  position: 'absolute',
  transform: 'translateY(-50%)',
})

// TextInput draws its own clear button here, so the browser's would make two.
export const inputWithClear = style({
  paddingInlineEnd: '36px',
  selectors: {
    '&::-webkit-search-cancel-button': { display: 'none' },
  },
})

export const clearButton = style({
  insetBlockStart: '50%',
  insetInlineEnd: '6px',
  transform: 'translateY(-50%)',
  // The button's own `position: relative` is an equal wax rule; doubling wins in any CSS order.
  selectors: { '&&': { position: 'absolute' } },
})
