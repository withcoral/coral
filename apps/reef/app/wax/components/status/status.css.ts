import { recipe, style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

// Capped at its container, so a narrow table cell truncates the pill's text
// instead of clipping the pill.
export const root = style({
  display: 'inline-flex',
  flexDirection: 'column',
  gap: 4,
  maxWidth: '100%',
  minWidth: 0,
  verticalAlign: 'middle',
})
export const pills = style({ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 6 })

export const dot = recipe({
  base: { display: 'inline-flex', flexShrink: 0 },
  variants: {
    tone: {
      ready: { color: theme.pill.green.color },
      blocked: { color: theme.pill.red.color },
      account: { color: theme.pill.amber.color },
    },
  },
})
