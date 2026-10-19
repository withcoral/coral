import { style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

// Every part is set in the button-strong style: links at their 32 size, the
// current page and the separators to match.
export const breadcrumbList = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 4,
  minWidth: 0,
})

export const breadcrumbItem = style({
  alignItems: 'center',
  display: 'flex',
  gap: 4,
  minWidth: 0,
  maxWidth: '100%',
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
