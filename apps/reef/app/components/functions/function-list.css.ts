import { style } from '@vanilla-extract/css'

export const table = style({
  tableLayout: 'fixed',
})

export const nameColumn = style({
  width: '24%',
})

export const sourcesColumn = style({
  width: '30%',
})

export const actionsColumn = style({
  width: 52,
})

export const row = style({})

export const cellContent = style({
  display: 'block',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const action = style({
  '@media': {
    '(hover: none)': {
      opacity: 1,
      pointerEvents: 'auto',
    },
  },
  display: 'flex',
  justifyContent: 'flex-end',
  opacity: 0,
  pointerEvents: 'none',
  transition: 'opacity 100ms ease',
  selectors: {
    [`${row}:focus-within &`]: {
      opacity: 1,
      pointerEvents: 'auto',
    },
    [`${row}:hover &`]: {
      opacity: 1,
      pointerEvents: 'auto',
    },
  },
})
