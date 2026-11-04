import { keyframes, recipe, style, styleVariants } from '@/wax/css'

import { theme } from '@/wax/theme/theme.css'

/** A bar's fill by tone. Pair a tone with text or a shape: it never carries meaning alone. */
export const toneFill = {
  neutral: theme.chart.neutral.fill,
  blue: theme.chart.blue.fill,
  purple: theme.chart.purple.fill,
  amber: theme.chart.amber.fill,
  red: theme.chart.red.fill,
}

export const axis = style({
  position: 'relative',
  height: '100%',
  overflow: 'hidden',
})

export const tick = style({
  position: 'absolute',
  top: 0,
  bottom: 0,
  display: 'flex',
  alignItems: 'center',
  paddingLeft: 4,
  whiteSpace: 'nowrap',
  fontFamily: theme.typography.bodySmall.fontFamily,
  fontSize: theme.typography.bodySmall.fontSize,
  color: theme.content.tertiary,
  borderLeft: `1px solid ${theme.stroke.secondary}`,
  fontVariantNumeric: 'tabular-nums',
})

export const grid = style({
  position: 'absolute',
  inset: 0,
  pointerEvents: 'none',
  overflow: 'hidden',
})

export const gridLine = style({
  position: 'absolute',
  top: 0,
  bottom: 0,
  borderLeft: `1px solid ${theme.stroke.secondary}`,
  opacity: 0.6,
})

export const bar = style({
  position: 'absolute',
  top: '50%',
  height: 12,
  marginTop: -6,
  minWidth: 2,
  borderRadius: 2,
  boxSizing: 'border-box',
})

export const barTone = styleVariants(toneFill, (fill) => ({ background: fill }))

/** A bracket over the bars under it: a thin line with a short leg at each end. */
export const bracketTone = styleVariants(toneFill, (fill) => ({
  height: 6,
  marginTop: -1,
  background: 'transparent',
  border: `1.5px solid ${fill}`,
  borderBottom: 'none',
  borderRadius: '2px 2px 0 0',
}))

/**
 * The time a call waited before dispatch: hatched amber, thinner than the run. Waiting is never
 * the call's outcome, so it never takes a failure's red.
 */
export const wait = style({
  height: 8,
  marginTop: -4,
  // Muted: waiting is the most harmless state, so it must not draw the eye like a hazard.
  opacity: 0.55,
  background: `repeating-linear-gradient(135deg, ${toneFill.amber} 0 2px, transparent 2px 6px)`,
  border: `1px solid ${toneFill.amber}`,
  borderRight: 'none',
  borderRadius: '2px 0 0 2px',
})

const pulse = keyframes({
  '0%, 100%': { opacity: 1 },
  '50%': { opacity: 0.35 },
})

/** An ongoing interval has no right cap: its end moves with the clock. */
export const ongoing = style({
  borderTopRightRadius: 0,
  borderBottomRightRadius: 0,
  selectors: {
    '&::after': {
      content: '""',
      position: 'absolute',
      right: -3,
      top: -2,
      bottom: -2,
      width: 2,
      background: theme.content.primary,
      animation: `${pulse} 1.6s ease-in-out infinite`,
    },
  },
  '@media': {
    '(prefers-reduced-motion: reduce)': {
      selectors: { '&::after': { animation: 'none' } },
    },
  },
})

/** How far an interval whose end is unknown shows before it fades out, in pixels. */
export const UNKNOWN_WIDTH = 24
/** An interval whose end Coral does not know fades out instead of ending. */
export const unknown = style({
  width: UNKNOWN_WIDTH,
  maskImage: 'linear-gradient(to right, black 30%, transparent)',
})

export const label = recipe({
  base: {
    position: 'absolute',
    top: 0,
    bottom: 0,
    display: 'flex',
    alignItems: 'center',
    whiteSpace: 'nowrap',
    fontFamily: theme.typography.bodySmall.fontFamily,
    fontSize: theme.typography.bodySmall.fontSize,
    color: theme.content.secondary,
    fontVariantNumeric: 'tabular-nums',
    pointerEvents: 'none',
  },
  variants: {
    side: {
      after: { paddingLeft: 6 },
      before: { paddingRight: 6, justifyContent: 'flex-end' },
      // Over the bar's end, on a chip of the page's surface so it reads on any tone.
      inside: {
        top: 3,
        bottom: 3,
        marginRight: 4,
        padding: '0 4px',
        borderRadius: 3,
        background: theme.surface.mainContent,
        display: 'block',
        lineHeight: '18px',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
      },
    },
  },
})

export const mark = recipe({
  base: {
    all: 'unset',
    position: 'absolute',
    top: '50%',
    width: 8,
    height: 8,
    marginTop: -4,
    marginLeft: -4,
    cursor: 'pointer',
    background: theme.content.primary,
    outline: `1.5px solid ${theme.surface.mainContent}`,
    selectors: {
      '&:focus-visible': { outline: `2px solid ${theme.stroke.focused}`, outlineOffset: 1 },
    },
  },
  variants: {
    // Shape, not color, tells a log, an emission and a lifecycle Event apart.
    shape: {
      diamond: { transform: 'rotate(45deg)', borderRadius: 1 },
      circle: { borderRadius: '50%' },
      square: { borderRadius: 1 },
    },
  },
})

export const minimap = style({
  position: 'relative',
  height: 56,
  borderRadius: 6,
  border: `1px solid ${theme.stroke.secondary}`,
  overflow: 'hidden',
  cursor: 'crosshair',
  userSelect: 'none',
  touchAction: 'none',
  selectors: {
    '&:focus-visible': { outline: `2px solid ${theme.stroke.focused}`, outlineOffset: 2 },
  },
})

export const lane = style({ position: 'absolute', minWidth: 1, borderRadius: 1 })

export const shade = style({
  position: 'absolute',
  top: 0,
  bottom: 0,
  background: theme.surface.backdrop,
  pointerEvents: 'none',
})

// The visible range: a quiet outline over the shading, so it does not read as focus.
export const window = style({
  position: 'absolute',
  top: 0,
  bottom: 0,
  border: `1px solid ${theme.stroke.primary}`,
  borderRadius: 4,
  boxSizing: 'border-box',
  pointerEvents: 'none',
})

/** Agent time between calls: a dotted line, quieter than any bar. */
const dots = `radial-gradient(circle, ${theme.content.tertiary} 1px, transparent 1.5px) 0 50% / 5px 5px repeat-x`

export const gap = style({
  position: 'absolute',
  top: '50%',
  height: 6,
  marginTop: -3,
  background: dots,
})

/** On the dotted line, on a chip of the page's surface, so it reads in a row's height. */
/** The stretch that matters most, such as the agent's time after its last call. */
export const gapEmphasis = style({
  height: 8,
  marginTop: -4,
  background: `radial-gradient(circle, ${theme.content.primary} 1.5px, transparent 2px) 0 50% / 7px 7px repeat-x`,
})
export const gapLabelEmphasis = style({ color: theme.content.primary, fontWeight: 600 })

export const gapLabel = style({
  position: 'absolute',
  top: '50%',
  padding: '0 4px',
  borderRadius: 3,
  background: theme.surface.mainContent,
  whiteSpace: 'nowrap',
  fontFamily: theme.typography.bodySmall.fontFamily,
  fontSize: theme.typography.bodySmall.fontSize,
  color: theme.content.tertiary,
  fontVariantNumeric: 'tabular-nums',
  pointerEvents: 'none',
})

/** A folded stretch of time: a faint band behind every row, so a break reads as a break. */
export const breakBand = style({
  position: 'absolute',
  top: 0,
  bottom: 0,
  background: theme.surface.onMainContentSubtle,
  borderInline: `1px dashed ${theme.stroke.secondary}`,
})

/** A zigzag where the axis folds a stretch away. */
export const axisBreak = style({
  position: 'absolute',
  top: '50%',
  transform: 'translate(-50%, -50%)',
  color: theme.content.tertiary,
})

/** A mark's shape in a key: the same look, standing in the line of text. */
export const markKey = recipe({
  base: {
    display: 'inline-block',
    width: 7,
    height: 7,
    marginInline: 2,
    background: theme.content.primary,
  },
  variants: {
    shape: {
      diamond: { transform: 'rotate(45deg)', borderRadius: 1 },
      circle: { borderRadius: '50%' },
      square: { borderRadius: 1 },
    },
  },
})
