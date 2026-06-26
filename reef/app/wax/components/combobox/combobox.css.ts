import { keyframes, style } from '@vanilla-extract/css'

import { animation, theme } from '@/wax/theme/theme.css'
import { zIndex } from '@/wax/theme/z-index'

const fadeIn = keyframes({
  from: {
    opacity: 0,
    transform: 'scale(0.95)',
  },
  to: {
    opacity: 1,
    transform: 'scale(1)',
  },
})

const fadeOut = keyframes({
  from: {
    opacity: 1,
    transform: 'scale(1)',
  },
  to: {
    opacity: 0,
    transform: 'scale(0.95)',
  },
})

export const positioner = style({
  zIndex: zIndex.tooltip,
})

export const popup = style({
  backgroundColor: theme.surface.floating,
  border: `1px solid ${theme.stroke.floating}`,
  borderRadius: '10px',
  boxShadow: theme.elevation.e3,
  display: 'flex',
  flexDirection: 'column',
  gap: '2px',
  maxHeight: '400px',
  outline: 'none',
  overflowX: 'hidden',
  overflowY: 'auto',
  padding: '4px',
  selectors: {
    '&[data-closed]': {
      animation: `${fadeOut} 0.1s ease-in`,
    },
    '&[data-open]': {
      animation: `${fadeIn} 0.15s ease-out`,
    },
  },
  transformOrigin: 'var(--transform-origin)',
  width: 'var(--anchor-width)',
})

export const list = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '2px',
  outline: 'none',
})

export const item = style({
  alignItems: 'center',
  borderRadius: '8px',
  color: theme.content.primary,
  cursor: 'pointer',
  display: 'flex',
  gap: '10px',
  height: '32px',
  outline: 'none',
  overflow: 'clip',
  paddingBlock: '6px',
  paddingInlineEnd: '2px',
  paddingInlineStart: '8px',
  textDecoration: 'none',
  transition: animation.colorTransition,
  width: '100%',
  ...theme.typography.body,
  selectors: {
    '&[data-disabled]': {
      color: theme.content.disabled,
      cursor: 'not-allowed',
    },
    '&[data-hidden]': {
      display: 'none',
    },
    '&[data-highlighted]': {
      backgroundColor: theme.surface.onMainContent,
    },
  },
})

export const itemLabel = style({
  flex: '1 0 0',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const itemIndicator = style({
  color: theme.content.tertiary,
  display: 'flex',
  flexShrink: 0,
  marginInlineEnd: '4px',
  marginInlineStart: 'auto',
})

export const input = style({
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
})

export const inputBare = style({
  alignSelf: 'center',
  backgroundColor: 'transparent',
  blockSize: '20px',
  border: 'none',
  boxShadow: 'none',
  color: theme.content.primary,
  flex: '1 0 24px',
  minWidth: '24px',
  outline: 'none',
  padding: '0',
  width: 'auto',
  ...theme.typography.body,
  lineHeight: '20px',
  selectors: {
    '&::placeholder': {
      color: theme.content.placeholder,
    },
    '&:disabled': {
      color: theme.content.disabled,
      cursor: 'not-allowed',
    },
  },
})

export const inputGroup = style({
  alignItems: 'center',
  backgroundColor: 'transparent',
  border: `1px solid ${theme.input.stroke.default}`,
  borderRadius: '8px',
  boxShadow: 'none',
  display: 'flex',
  flexWrap: 'wrap',
  gap: '4px',
  paddingBlock: '7px',
  paddingInline: '8px',
  selectors: {
    '&:focus-within': {
      borderColor: theme.input.stroke.focus,
    },
    '&:hover:not(:focus-within)': {
      borderColor: theme.input.stroke.hover,
    },
  },
  transition: animation.colorTransition,
  width: '100%',
})

export const chips = style({
  alignItems: 'center',
  display: 'flex',
  flex: '1 1 auto',
  flexWrap: 'wrap',
  gap: '4px',
  minWidth: 0,
})

export const chip = style({
  alignItems: 'center',
  backgroundColor: theme.pill.graySubtle.background,
  borderRadius: '6px',
  color: theme.content.primary,
  cursor: 'default',
  display: 'inline-flex',
  fontFamily: theme.typography.bodySmallStrong.fontFamily,
  fontSize: '12px',
  fontWeight: theme.typography.bodySmallStrong.fontWeight,
  gap: '4px',
  letterSpacing: theme.typography.bodySmallStrong.letterSpacing,
  lineHeight: '16px',
  maxWidth: '100%',
  minWidth: 0,
  paddingBlock: '2px',
  paddingInline: '6px',
})

export const chipLabel = style({
  display: 'block',
  flex: '1 1 auto',
  maxWidth: '180px',
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const chipRemove = style({
  alignItems: 'center',
  backgroundColor: 'transparent',
  border: 'none',
  borderRadius: '4px',
  color: theme.content.tertiary,
  cursor: 'pointer',
  display: 'inline-flex',
  outline: 'none',
  padding: '0',
  selectors: {
    '&:hover:not([data-disabled])': {
      color: theme.content.secondary,
    },
    '&[data-disabled]': {
      cursor: 'not-allowed',
      opacity: 0.5,
    },
  },
})

export const empty = style({
  color: theme.content.tertiary,
  paddingBlock: '6px',
  paddingInline: '8px',
  ...theme.typography.body,
  selectors: {
    '&:empty': {
      display: 'none',
    },
    '&[data-hidden]': {
      display: 'none',
    },
  },
})
