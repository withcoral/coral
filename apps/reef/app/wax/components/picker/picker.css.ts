import { popup as menuPopup } from '@/wax/components/menu/menu.css'
import { style } from '@/wax/css'

export const label = style({
  paddingBlockEnd: '6px',
  paddingInline: '8px',
})

// Fits the longest option instead of the menu's fixed width, and never sits
// narrower than the trigger or wider than the viewport allows.
export const popup = style({
  selectors: {
    [`${menuPopup}&`]: {
      maxWidth: 'min(360px, var(--available-width))',
      minWidth: 'max(180px, var(--anchor-width))',
      width: 'max-content',
    },
  },
})
