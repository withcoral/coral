import { style } from '@/wax/css'

export const label = style({
  paddingBlockEnd: '6px',
  paddingInline: '8px',
})

export const field = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '6px',
})

// Holds the chevron at the far edge once the trigger is wider than its text.
export const trigger = style({
  justifyContent: 'space-between',
})

// Squares the option list up with the trigger it hangs from, and keeps it
// inside the viewport on a narrow screen.
export const menu = style({
  maxWidth: 'var(--available-width)',
  minWidth: 'var(--anchor-width)',
})
