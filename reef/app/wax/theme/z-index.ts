/* eslint-disable perfectionist/sort-objects -- we want this list to be sorted by their value */
/**
 * Centralized z-index scale for consistent layering.
 *
 * Strategy: Count down from MAX_ZINDEX (max 32-bit signed int) to guarantee
 * our UI sits above third-party widgets, iframes, and other content.
 * Gaps of 2 between levels allow inserting new layers without renumbering.
 *
 * Hierarchy (lowest to highest):
 * - behind: Elements that must stay behind everything
 * - base: Default stacking context reset
 * - raised: Slightly elevated elements (sticky headers in tables)
 * - sticky: Page-level sticky elements
 * - navigation: Sidebar, main navigation
 * - floating: Floating toolbars, inline menus
 * - portaledMenu: Menus portaled to document body
 * - modalBackdrop: Modal backdrop overlay
 * - modal: Modal dialog content
 * - tooltip: Tooltips, popovers (above modals so tooltips in modals are visible)
 * - notification: Toast notifications (reserved for react-toastify)
 * - critical: Admin overlays, system-critical UI
 */
const MAX_ZINDEX = 2147483647

export const zIndex = {
  behind: -1,
  base: 0,
  raised: 1,
  sticky: MAX_ZINDEX - 20,
  navigation: MAX_ZINDEX - 18,
  floating: MAX_ZINDEX - 16,
  portaledMenu: MAX_ZINDEX - 14,
  modalBackdrop: MAX_ZINDEX - 12,
  modal: MAX_ZINDEX - 10,
  tooltip: MAX_ZINDEX - 8,
  notification: MAX_ZINDEX - 4,
  critical: MAX_ZINDEX - 2,
} as const
