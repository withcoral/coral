import { style } from '@/wax/css'

/** Content only assistive technology reads, such as a polite live count or a hidden input. */
export const visuallyHidden = style({
  border: 0,
  clip: 'rect(0 0 0 0)',
  clipPath: 'inset(50%)',
  height: 1,
  margin: -1,
  overflow: 'hidden',
  padding: 0,
  position: 'absolute',
  whiteSpace: 'nowrap',
  width: 1,
})
