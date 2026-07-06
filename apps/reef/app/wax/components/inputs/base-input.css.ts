import { style } from '@vanilla-extract/css'

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
      '&:focus': {
        borderColor: theme.input.stroke.focus,
      },
      '&:hover:not(:focus):not(:disabled)': {
        borderColor: theme.input.stroke.hover,
      },
    },
  },
])

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
