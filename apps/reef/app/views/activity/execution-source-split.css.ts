import { style } from '@vanilla-extract/css'
import { theme } from '@/wax/theme/theme.css'

// Grows to fill whatever vertical room the drawer actually has left, up to
// its own max-height cap, instead of a fixed card size disconnected from the
// real available space; `minHeight` only keeps the split usable when the
// drawer is short. The two panes inside own their own scrolling, so the
// drawer itself does not also scroll past this point.
export const root = style({
  display: 'flex',
  flexDirection: 'column',
  flex: '1 1 auto',
  minHeight: 360,
  minWidth: 0,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  overflow: 'hidden',
})
export const split = style({
  flex: 1,
  minHeight: 0,
  minWidth: 0,
  display: 'flex',
  flexDirection: 'column',
})
// `flexGrow`/`flexBasis: 0` (set inline, per the live split ratio) divide whatever
// height `.split` actually has; `minHeight` keeps each pane usable even when the
// other is dragged toward it or the drawer itself shrinks, with no JS measurement.
// A column: the record's evidence scrolls, and its decision footer stays below it.
export const detailPane = style({
  position: 'relative',
  display: 'flex',
  flexDirection: 'column',
  minHeight: 120,
  minWidth: 0,
  overflow: 'hidden',
})
// The selected record's detail, header and decision footer included, that slides as one card.
export const detailStage = style({
  display: 'flex',
  flexDirection: 'column',
  flex: 1,
  minHeight: 0,
  minWidth: 0,
})
// The leaving record's copy slides over the pane, which clips it above the divider.
export const detailLeaving = style({
  position: 'absolute',
  inset: 0,
  display: 'flex',
  flexDirection: 'column',
})
export const detailRegion = style({
  display: 'flex',
  flexDirection: 'column',
  flex: 1,
  minHeight: 0,
  minWidth: 0,
})
export const detailScroll = style({ flex: 1, minHeight: 0 })
// Nothing to decide leaves no empty bar under the evidence.
export const detailFooter = style({
  flexShrink: 0,
  borderTop: `1px solid ${theme.stroke.secondary}`,
  padding: '12px 16px',
  selectors: { '&:empty': { display: 'none' } },
})
export const detailContent = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
  padding: 16,
})
export const sourcePane = style({
  minHeight: 120,
  minWidth: 0,
  padding: '12px 16px',
})
