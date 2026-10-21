import { style } from '@/wax/css'

/**
 * The rules behind `visuallyHidden`, for a style that hides content only under
 * a condition, such as inside a container query.
 */
export const visuallyHiddenRules = {
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
} as const

/** Content only assistive technology reads, such as a polite live count or a hidden input. */
export const visuallyHidden = style(visuallyHiddenRules)
