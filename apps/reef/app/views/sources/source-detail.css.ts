import { style } from '@vanilla-extract/css'

export const fieldGroup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
  paddingBlockStart: 16,
})

export const removeConfirmText = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  minWidth: 0,
})

export const removeConfirmActions = style({
  display: 'flex',
  flexShrink: 0,
  gap: 10,
  justifyContent: 'flex-end',
})
