import { style } from '@vanilla-extract/css'

export const grid = style({
  display: 'grid',
  gap: '16px',
  gridTemplateColumns: 'repeat(auto-fit, minmax(min(300px, 100%), 1fr))',
  listStyle: 'none',
  margin: 0,
  padding: 0,
})

export const item = style({
  display: 'flex',
})
