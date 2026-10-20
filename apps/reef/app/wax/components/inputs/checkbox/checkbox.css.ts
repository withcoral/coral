import { style } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

export const label = style({
  alignItems: 'center',
  color: theme.content.primary,
  cursor: 'pointer',
  display: 'inline-flex',
  gap: 8,
  ...theme.typography.body,
  selectors: { '&:has(:disabled)': { color: theme.content.disabled, cursor: 'not-allowed' } },
})
export const fill = style({
  alignSelf: 'stretch',
  height: '100%',
  justifyContent: 'center',
  width: '100%',
})
// Drawn here rather than by the browser, whose unchecked outline is near-black.
// The outline keeps 3:1 against the page (WCAG 1.4.11); a check or a dash in
// the filled box marks the state, so it does not rest on colour alone.
const CHECK =
  "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Cpath d='M3.5 8.5l3 3 6-7' fill='none' stroke='black' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E\")"
const DASH =
  "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Cpath d='M4 8h8' fill='none' stroke='black' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E\")"

export const input = style({
  appearance: 'none',
  background: theme.surface.mainContent,
  border: `1px solid ${theme.stroke.control}`,
  borderRadius: 4,
  cursor: 'inherit',
  display: 'grid',
  flexShrink: 0,
  height: 16,
  margin: 0,
  placeContent: 'center',
  width: 16,
  '::before': {
    background: theme.surface.mainContent,
    content: '""',
    height: 12,
    maskPosition: 'center',
    maskRepeat: 'no-repeat',
    maskSize: 'contain',
    visibility: 'hidden',
    width: 12,
  },
  selectors: {
    '&:checked, &:indeterminate': {
      background: theme.stroke.focused,
      borderColor: theme.stroke.focused,
    },
    '&:checked::before': { maskImage: CHECK, visibility: 'visible' },
    '&:indeterminate::before': { maskImage: DASH, visibility: 'visible' },
    '&:disabled': { borderColor: theme.stroke.secondary, cursor: 'not-allowed' },
    '&:disabled:checked, &:disabled:indeterminate': {
      background: theme.content.disabled,
      borderColor: theme.content.disabled,
    },
    '&:focus-visible': { outline: `2px solid ${theme.stroke.focused}`, outlineOffset: 2 },
  },
})
