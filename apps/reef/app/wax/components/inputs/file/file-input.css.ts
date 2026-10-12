import { style } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

export const dropZone = style({
  alignItems: 'center',
  border: `1px dashed ${theme.input.stroke.default}`,
  borderRadius: 8,
  color: theme.content.primary,
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
  padding: 16,
  position: 'relative',
  textAlign: 'center',
  selectors: {
    '&:hover:not([data-disabled])': { borderColor: theme.input.stroke.hover },
    '&[data-dropping]:not([data-disabled])': {
      background: theme.surface.card,
      borderStyle: 'solid',
      borderColor: theme.input.stroke.focus,
    },
    '&[data-invalid]:not([data-disabled])': { borderColor: theme.input.stroke.error },
    '&:focus-within': { outline: `2px solid ${theme.input.stroke.focus}`, outlineOffset: 2 },
    '&[data-disabled], &:has(input:disabled)': {
      borderColor: theme.input.stroke.disabled,
      color: theme.content.disabled,
      cursor: 'not-allowed',
    },
  },
})

/** The zone dims every line at once, so each one names the disabled colour. */
const dimmed = {
  [`${dropZone}[data-disabled] &`]: { color: theme.content.disabled },
  [`${dropZone}:has(input:disabled) &`]: { color: theme.content.disabled },
}

export const instructions = style({
  color: theme.content.tertiary,
  margin: 0,
  ...theme.typography.bodySmall,
  selectors: { ...dimmed },
})
export const filename = style({
  color: theme.content.secondary,
  margin: 0,
  maxWidth: '100%',
  overflowWrap: 'anywhere',
  ...theme.typography.bodySmallStrong,
  selectors: { '&:empty': { display: 'none' }, ...dimmed },
})
