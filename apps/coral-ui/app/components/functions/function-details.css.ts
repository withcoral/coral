import { style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'

export const root = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  height: '100%',
  minHeight: 0,
  minWidth: 0,
})

export const scrollBody = style({
  flex: 1,
  minHeight: 0,
})

export const content = style({
  boxSizing: 'border-box',
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 20,
  marginInline: 'auto',
  maxWidth: 960,
  minHeight: '100%',
  minWidth: 0,
  padding: 16,
  width: '100%',
})

export const intro = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
})

export const name = style({
  margin: 0,
  overflowWrap: 'anywhere',
})

export const description = style({
  margin: 0,
  maxWidth: 680,
})

export const sources = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  marginBlockStart: 4,
  minWidth: 0,
})

export const sourcePills = style({
  flex: 1,
  minWidth: 0,
})

export const shapeGrid = style({
  display: 'grid',
  gap: 16,
  gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gridTemplateColumns: 'minmax(0, 1fr)',
    },
  },
})

export const shapeSection = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
})

export const shapeEmpty = style({
  alignItems: 'center',
  display: 'flex',
  minHeight: 32,
  paddingBlock: 6,
  paddingInline: 12,
})

export const section = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
})
