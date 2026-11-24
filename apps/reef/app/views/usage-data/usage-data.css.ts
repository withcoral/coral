import { style } from '@/wax/css'

// Holds the disabled switch, so a hover or focus on it shows why sharing is off.
export const blockedSwitch = style({ borderRadius: 100, display: 'inline-flex' })

export const report = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
  paddingTop: '12px',
})
