import { style } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

export const record = style({
  display: 'flex',
  alignItems: 'center',
  gap: 6,
  minWidth: 0,
})
export const control = style({ display: 'flex', flex: '0 0 24px' })
export const content = style({
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  minWidth: 0,
  flex: 1,
})
export const label = style({
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
export const metadata = style({
  display: 'flex',
  alignItems: 'center',
  gap: 4,
  ...theme.typography.bodySmall,
  color: theme.content.secondary,
  flexShrink: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})
export const marker = style({
  ...theme.typography.bodySmall,
  color: theme.content.primary,
  background: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 12,
  flexShrink: 0,
  padding: '2px 8px',
  whiteSpace: 'nowrap',
})
