import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const container = style({
  display: 'block',
  height: 'auto',
  maxWidth: '100%',
})

export const axisText = style({
  fill: theme.content.secondary,
  fontFamily: theme.typography.bodySmall.fontFamily,
  fontSize: '11px',
})

export const gridLine = style({
  stroke: theme.stroke.secondary,
  strokeDasharray: '3 3',
})
