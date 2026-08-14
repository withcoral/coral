import { style } from '@vanilla-extract/css'

import { theme } from '@/wax/theme/theme.css'

export const breadcrumbList = style({
  alignItems: 'center',
  display: 'flex',
  gap: 4,
  padding: '6px 0',
})

export const breadcrumbItem = style({
  alignItems: 'center',
  display: 'flex',
  gap: 4,
  selectors: {
    '&:not(:last-child)::after': {
      ...theme.typography.buttonStrong,
      color: theme.content.tertiary,
      content: '/',
    },
  },
})

export const breadcrumbItemContent = style({
  flexShrink: 1,
  minInlineSize: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
