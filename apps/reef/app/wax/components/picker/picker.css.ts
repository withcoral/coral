import { popup as menuPopup } from '@/wax/components/menu/menu.css'
import { style } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

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

// A secondary button has no border, so an inset ring draws the error border
// that a text input draws.
export const invalid = style({
  boxShadow: `inset 0 0 0 1px ${theme.input.stroke.error}`,
})
