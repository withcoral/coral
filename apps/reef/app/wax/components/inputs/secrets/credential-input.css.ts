import { style } from '@/wax/css'

// Room at the end of the input for the reveal button.
export const input = style({ paddingInlineEnd: '36px' })

export const reveal = style({
  insetBlockStart: '50%',
  insetInlineEnd: '4px',
  position: 'absolute',
  transform: 'translateY(-50%)',
})
