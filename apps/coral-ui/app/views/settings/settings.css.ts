import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'

const MOBILE_QUERY = `screen and (max-width: ${breakpoints.mobile})`

// The pane itself never scrolls. Its heading stays put and only the body below
// moves, so the search box keeps its place next to the rows it filters.
export const page = style({
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  minHeight: 0,
  overflow: 'hidden',
})

export const header = style({
  flexShrink: 0,
})

// The band spans the pane so the scrollbar rides its edge, while the column
// inside it takes the same width and padding as the body underneath.
export const headerInner = style({
  alignItems: 'flex-start',
  boxSizing: 'border-box',
  display: 'flex',
  gap: '8px',
  marginInline: 'auto',
  maxWidth: '960px',
  paddingBlockEnd: '24px',
  paddingBlockStart: '32px',
  paddingInline: '24px',
  width: '100%',
  '@media': {
    [MOBILE_QUERY]: {
      flexDirection: 'column',
      gap: '16px',
      paddingBlockEnd: '16px',
      paddingBlockStart: '20px',
      paddingInline: '16px',
    },
  },
})

export const headerText = style({
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: '4px',
  minWidth: 0,
})

export const searchBar = style({
  flex: '0 1 280px',
  marginInlineStart: 'auto',
  maxWidth: '280px',
  minWidth: '180px',
  '@media': {
    [MOBILE_QUERY]: {
      maxWidth: '100%',
      width: '100%',
    },
  },
})

export const scroll = style({
  flex: 1,
  minHeight: 0,
})

export const body = style({
  boxSizing: 'border-box',
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: '24px',
  marginInline: 'auto',
  maxWidth: '960px',
  minHeight: '100%',
  paddingBlockEnd: '32px',
  paddingInline: '24px',
  width: '100%',
  '@media': {
    [MOBILE_QUERY]: {
      gap: '20px',
      paddingBlockEnd: '20px',
      paddingInline: '16px',
    },
  },
})
