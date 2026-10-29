import { createVar, globalStyle, keyframes, style, styleVariants } from '@/wax/css'

import { utils } from '@/styles/utils'
import { stickyBottom, stickyTop } from '@/wax/components/card/card.css'
import { animation, staticUtilities, theme, zIndex } from '@/wax/theme/theme.css'

export const backdrop = style({
  backgroundColor: staticUtilities.black,
  inset: 0,
  opacity: 0.2,
  position: 'fixed',
  selectors: {
    '&[data-starting-style], &[data-ending-style]': {
      opacity: 0,
    },
  },
  transition: 'opacity 150ms cubic-bezier(0.45, 1.005, 0, 1.005)',
  zIndex: zIndex.modalBackdrop,
})

// The sticky header offsets this padding so it stays flush with the popup edge.
const padding = createVar()

export const popup = style({
  backgroundColor: theme.surface.floating,
  border: `1px solid ${theme.stroke.floating}`,
  borderRadius: '10px',
  boxShadow: theme.elevation.e4,
  display: 'flex',
  flexDirection: 'column',
  gap: '12px',
  insetBlockStart: '50%',
  insetInlineStart: '50%',
  maxHeight: 'calc(100vh - 64px)',
  maxWidth: 'calc(100vw - 48px)',
  overflow: 'auto',
  padding,
  position: 'fixed',
  selectors: {
    '&[data-nested-dialog-open]::after': {
      backgroundColor: utils.opacify(staticUtilities.black, 0.05),
      borderRadius: 'inherit',
      content: '',
      inset: 0,
      position: 'absolute',
    },
    '&[data-starting-style], &[data-ending-style]': {
      opacity: 0,
      transform: 'translate(-50%, -50%) scale(0.9)',
    },
  },
  transform: 'translate(-50%, -50%) scale(calc(1 - 0.1 * var(--nested-dialogs)))',
  transition: 'all 150ms',
  translate: '0 calc(0px + 16px * var(--nested-dialogs))',
  vars: { [padding]: '24px' },
  zIndex: zIndex.modal,
})

// Opening is a keyframe rather than a transition from `data-starting-style`, because
// a drawer that a route renders mounts already open and Base UI skips that phase.
const drawerTravel = createVar()
const { panel } = animation
// A docked drawer moves the page beside it, so it opens and closes in half the time.
const drawerOpen = createVar()
const drawerClose = createVar()
const dockedOpen = '200ms'
const dockedClose = '175ms'
const drawerHidden = {
  filter: panel.blur,
  transform: `translateX(${drawerTravel})`,
}
const drawerReveal = keyframes({ from: drawerHidden })

export const drawer = style({
  insetBlockStart: 0,
  insetInlineStart: 0,
  height: '100dvh',
  maxHeight: '100dvh',
  maxWidth: 'calc(100vw - 48px)',
  borderRadius: 0,
  vars: {
    [padding]: '12px',
    [drawerTravel]: '-100%',
    [drawerOpen]: panel.open,
    [drawerClose]: panel.close,
  },
  transform: 'none',
  translate: 'none',
  animation: `${drawerReveal} ${drawerOpen} ${panel.ease}`,
  transition: 'none',
  selectors: {
    // The drawer takes focus so its arrow keys work; it is not a control, so it shows no ring.
    '&:focus-visible': { outline: 'none' },
    '&[data-starting-style]': { opacity: 1, transform: 'none' },
    '&[data-ending-style]': {
      ...drawerHidden,
      opacity: 1,
      transition: `transform ${drawerClose} ${panel.ease}, filter ${drawerClose} ${panel.ease}`,
    },
  },
  '@media': {
    '(prefers-reduced-motion: reduce)': {
      animation: 'none',
      selectors: { '&[data-ending-style]': { transition: 'none' } },
    },
  },
})

export const rightDrawer = style({
  insetInlineEnd: 0,
  insetInlineStart: 'auto',
  vars: { [drawerTravel]: '100%' },
})

export const popupSize = styleVariants({
  l: { width: '600px' },
  m: { width: '400px' },
  xl: { width: '700px' },
  // Side-by-side values, such as a comparison table.
  xxl: { width: '960px' },
})

/** At this width or less a long-form popup fills the screen, and a rail is modal. */
export const mobile = '(max-width: 640px)'

export const fullWidthOnMobile = style({
  '@media': { [mobile]: { width: '100vw', maxWidth: '100vw' } },
})

// Beside the page, the rail sits under page menus, dialogs and tooltips.
export const besidePage = style({ zIndex: zIndex.rail })

/** At this width or more a docked rail takes its place in the page's layout. */
export const docked = '(min-width: 1200px)'

// Where a docked rail portals its popup. It has no box of its own until the rail
// docks, so a floating or modal rail still positions against the viewport.
export const dock = style({ display: 'contents' })

// The rail's width beside the page, and the space it keeps from it. The dock's basis
// takes its share of the row; the portal inside takes the same share in container units.
const dockShare = 'clamp(420px, 45%, 700px)'
const railWidth = 'clamp(420px, 45cqw, 700px)'
const dockGap = '16px'

// The page beside it keeps the rest of the row, so a docked rail takes a share of it.
// The dock widens from nothing as the rail opens and narrows as it closes, in step with
// the popup, so the page beside it resizes smoothly. A percentage stays a percentage
// once computed, so resizing the window or swapping the record moves nothing.
export const dockActive = style({
  display: 'flex',
  flexDirection: 'column',
  flex: `0 0 calc(${dockShare} + ${dockGap})`,
  minHeight: 0,
  minWidth: 0,
  // The rail keeps its width while the dock reveals it, so its text never rewraps.
  overflowX: 'clip',
  transition: `flex-basis ${dockedOpen} ${panel.ease}`,
  '@starting-style': { flexBasis: 0 },
  // A closed rail leaves the page the whole row.
  selectors: { '&:empty': { display: 'none' } },
  '@media': { '(prefers-reduced-motion: reduce)': { transition: 'none' } },
})

// The portal's own element passes the dock's height on to the popup, at the rail's full
// width. The dock's row is the container, so the width matches the dock's settled share.
globalStyle(`${dockActive} > *`, {
  display: 'flex',
  flexDirection: 'column',
  flex: '1 1 auto',
  minHeight: 0,
  width: railWidth,
  marginInlineStart: dockGap,
})
globalStyle(`:has(> ${dockActive})`, { containerType: 'inline-size' })

// In the page's flow, beside the content it pushes aside. It sets no stacking order,
// so a dialog opened from it still draws above the page. It keeps the raised surface,
// which its pinned headers and fades share, inside an outline instead of a shadow.
// The dock's width slides it in and out, so it does not travel itself; its blur keeps
// the close running until the dock has narrowed.
export const dockedPopup = style({
  borderColor: theme.stroke.secondary,
  position: 'relative',
  inset: 'auto',
  flex: '1 1 auto',
  width: 'auto',
  maxWidth: 'none',
  height: 'auto',
  maxHeight: 'none',
  minHeight: 0,
  borderRadius: 10,
  boxShadow: 'none',
  zIndex: 'auto',
  vars: { [drawerTravel]: '0px', [drawerOpen]: dockedOpen, [drawerClose]: dockedClose },
})

// Closing, the dock narrows back to nothing over the popup's own close.
globalStyle(`${dockActive}:has(${dockedPopup}[data-ending-style])`, {
  flexBasis: 0,
  transition: `flex-basis ${dockedClose} ${panel.ease}`,
  '@media': { '(prefers-reduced-motion: reduce)': { transition: 'none' } },
})

export const header = style({
  position: 'sticky',
  top: `calc(-1 * ${padding})`,
  zIndex: 2,
  flexShrink: 0,
  background: theme.surface.floating,
  paddingBlock: 12,
  paddingInlineEnd: 24,
})

export const title = style({
  color: theme.content.primary,
  flexShrink: 0,
  ...theme.typography.headingMedium,
  ...utils.boxClamp(1),
  marginInlineEnd: '24px',
})

export const description = style({
  color: theme.content.secondary,
  flexShrink: 0,
  ...theme.typography.body,
})

export const close = style({
  insetBlockStart: '16px',
  insetInlineEnd: '16px',
  position: 'absolute',
  selectors: { [`${header} &`]: { insetBlockStart: 12, insetInlineEnd: 0 } },
})

// The footer stays put, so a contained popup keeps its actions in view.
export const actions = style({
  display: 'flex',
  flexShrink: 0,
  gap: '10px',
  justifyContent: 'flex-end',
  marginBlockStart: '12px',
})

// Content that pins to the popup's top edge, flush like the dialog header. A card's sticky
// header reads the inset, so the popup's padding wins without depending on stylesheet order.
export const pinned = style({
  top: `calc(-1 * ${padding})`,
  vars: { [stickyTop]: `calc(-1 * ${padding})` },
})

// Content that pins to the popup's bottom edge, flush like `pinned` at the top.
export const pinnedBottom = style({
  bottom: `calc(-1 * ${padding})`,
  vars: { [stickyBottom]: `calc(-1 * ${padding})` },
})

// Content that reaches through the popup's side padding to its edges, such as a split's divider.
export const flushInline = style({ marginInline: `calc(-1 * ${padding})` })

// The close control placed in the flow of a header the content owns.
export const closeInline = style({ position: 'static', flexShrink: 0 })
