import { style } from '@vanilla-extract/css'

export const container = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: '4px',
})

export const overflowTrigger = style({
  display: 'inline-flex',
})

export const tooltipContent = style({
  alignItems: 'flex-start',
  display: 'flex',
  flexDirection: 'column',
  flexWrap: 'wrap',
  gap: '4px',
})
