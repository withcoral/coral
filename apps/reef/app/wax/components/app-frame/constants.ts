import { TOP_BAR_HEIGHT_PX } from '@/wax/components/page-layout/constants'

/** The gap between the page's frame and the window's edge, and the frame's line. */
export const FRAME_INSET_PX = 8
export const FRAME_LINE_PX = 1

/**
 * From the window's top edge to the centre line of the page's top bar, which
 * starts under the frame's inset and its line. The navigation puts its brand on
 * this line, so the brand and the breadcrumbs share it.
 */
export const TOP_BAR_CENTRE_PX = FRAME_INSET_PX + FRAME_LINE_PX + TOP_BAR_HEIGHT_PX / 2
