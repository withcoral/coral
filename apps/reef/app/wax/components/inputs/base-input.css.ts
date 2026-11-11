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

// With a prefix, the container draws the field's border, so the prefix sits
// inside it beside the value. It follows the input's own states.
export const prefixedContainer = style([
  baseInput,
  {
    gap: 0,
    paddingBlock: 0,
    // The input keeps its own end padding.
    paddingInlineEnd: 0,
    selectors: {
      // The whole field shows hover: the prefix and the padding as well as the input.
      '&:hover:not(:has(input:focus)):not(:has(input:disabled)):not(:has(input[data-invalid]))': {
        borderColor: theme.input.stroke.hover,
      },
      '&:has(input:focus:not([data-invalid]))': { borderColor: theme.input.stroke.focus },
      '&:has(input[data-invalid]:not(:disabled))': { borderColor: theme.input.stroke.error },
      '&:has(input:disabled)': {
        borderColor: theme.input.stroke.disabled,
        cursor: 'not-allowed',
      },
    },
  },
])

export const prefix = style({
  // Quieter than the value, so the reader sees where the value starts.
  color: theme.content.tertiary,
  cursor: 'text',
  flexShrink: 0,
  userSelect: 'none',
  whiteSpace: 'nowrap',
  selectors: {
    [`${prefixedContainer}:has(input:disabled) &`]: {
      color: theme.content.disabled,
      cursor: 'not-allowed',
    },
  },
})

// The container draws the border, so the input draws none and starts at the prefix.
// It keeps its own block padding: the container's border takes the place of its
// own, so the field is as tall as a plain one.
// Doubled, it outranks the input's own state rules in any CSS order.
export const prefixedInput = style({
  selectors: {
    '&&': {
      border: 'none',
      paddingInlineStart: 0,
    },
  },
})
