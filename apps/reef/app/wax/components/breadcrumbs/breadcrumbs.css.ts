import { style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

// The trail heads the page, a step above body text: each part is set at the
// body-large size, links at their 36 size and the current page in its strong weight.
export const breadcrumbList = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 4,
  minWidth: 0,
  padding: '6px 0',
})

export const breadcrumbItem = style({
  alignItems: 'center',
  display: 'flex',
  gap: 4,
  minWidth: 0,
  maxWidth: '100%',
  selectors: {
    '&:not(:last-child)::after': {
      ...theme.typography.bodyLarge,
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
