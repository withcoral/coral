import type { RefAttributes, SVGAttributes } from 'react'

/**
 * The props of a custom icon: those of an SVG element, and the `size` that an icon from
 * lucide-react takes. Stated here and not imported from lucide-react, so that
 * Storybook's react-docgen does not read that package for each icon.
 */
export interface CustomIconProps
  extends SVGAttributes<SVGSVGElement>, RefAttributes<SVGSVGElement> {
  size?: number | string
}
