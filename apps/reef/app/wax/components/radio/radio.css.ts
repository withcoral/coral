import { style } from '@vanilla-extract/css'

import { animation, theme } from '@/wax/theme/theme.css'

export const group = style({
  alignItems: 'center',
  display: 'flex',
  gap: 20,
  minWidth: 'max-content',
  paddingBlock: 4,
  paddingInline: 4,
})

export const item = style({
  alignItems: 'center',
  color: theme.content.secondary,
  cursor: 'pointer',
  display: 'inline-flex',
  flexShrink: 0,
  gap: 8,
  userSelect: 'none',
  whiteSpace: 'nowrap',
  ...theme.typography.body,
  selectors: {
    '&:hover:not(:has([data-disabled]))': {
      color: theme.content.primary,
    },
    '&:has([data-checked])': {
      color: theme.content.primary,
    },
    '&:has([data-disabled])': {
      color: theme.content.disabled,
      cursor: 'not-allowed',
    },
    '&:has([role="radio"]:focus-visible)': {
      borderRadius: 4,
      outline: `2px solid ${theme.stroke.focused}`,
      outlineOffset: 2,
    },
  },
})

export const control = style({
  alignItems: 'center',
  backgroundColor: 'transparent',
  border: `2px solid ${theme.content.tertiary}`,
  borderRadius: '50%',
  cursor: 'inherit',
  display: 'inline-flex',
  flexShrink: 0,
  height: 18,
  justifyContent: 'center',
  outline: 'none',
  padding: 0,
  transition: animation.colorTransition,
  width: 18,
  selectors: {
    '&[data-checked]': {
      borderColor: theme.stroke.focused,
    },
    '&[data-disabled]': {
      borderColor: theme.input.stroke.disabled,
    },
  },
})

export const indicator = style({
  backgroundColor: theme.stroke.focused,
  borderRadius: '50%',
  height: 8,
  width: 8,
})

export const label = style({
  pointerEvents: 'none',
})
