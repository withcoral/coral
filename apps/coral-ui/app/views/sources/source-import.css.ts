import { style } from '@vanilla-extract/css'

export const dialogContent = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '16px',
})

export const header = style({
  alignItems: 'center',
  gap: 10,
  marginBlockEnd: 14,
  paddingBlockEnd: 0,
})
