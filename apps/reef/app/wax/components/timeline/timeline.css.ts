import { createVar, fallbackVar, globalStyle, style, styleVariants } from '@/wax/css'
import { theme } from '@/wax/theme/theme.css'

/** The axis position the view starts at, and how much of the axis it shows. */
export const viewFrom = createVar()
export const viewSpan = createVar()
/** Where a bar, a mark or a fold starts on the axis, and how long it is. */
export const itemFrom = createVar()
export const itemLength = createVar()

const left = `calc((${itemFrom} - ${viewFrom}) / ${viewSpan} * 100%)`
const width = `calc(${itemLength} / ${viewSpan} * 100%)`

/** The shading every track draws where the view holds compressed time. */
export const foldsImage = createVar()
/** The colour of that shading, for the gradient the root computes. */
export const foldTint = createVar()

export const root = style({
  vars: {
    [viewFrom]: '0',
    [viewSpan]: '1',
    [foldsImage]: 'none',
    [foldTint]: theme.stroke.primary,
  },
  minWidth: 0,
})

/** One row's strip of the axis. It clips what the view does not show. */
export const track = style({
  position: 'relative',
  overflow: 'hidden',
  minWidth: 0,
  height: '100%',
  backgroundImage: foldsImage,
})

/** How far in from each side of a band its tracks start and end. */
export const foldsBandInset = createVar()
/**
 * Shades the folds across a whole surface, such as a table of tracks, as one band behind
 * what it holds: at the tracks' span, `foldsBandInset` in from each side. Its tracks then
 * draw no shading of their own.
 */
export const foldsBand = style({
  vars: { [foldsBandInset]: '0px' },
  backgroundImage: foldsImage,
  backgroundPosition: `${foldsBandInset} 0`,
  backgroundRepeat: 'no-repeat',
  backgroundSize: `calc(100% - 2 * ${foldsBandInset}) 100%`,
})
/** A track inside a band, which shades the folds for it. */
export const bandedTrack = style({ selectors: { [`&${track}`]: { backgroundImage: 'none' } } })
/** A quieter shade for the folds, for a band that lies behind text. */
export const subtleFolds = style({ vars: { [foldTint]: theme.surface.onMainContent } })

const barBase = style({
  position: 'absolute',
  insetBlock: 0,
  marginBlock: 'auto',
  left,
  width,
  // A very short interval still shows. Its exact time is in its description.
  minWidth: 3,
  height: 10,
  borderRadius: 3,
  boxSizing: 'border-box',
})
export const barColor = styleVariants({
  blue: { backgroundColor: theme.chart.blue.fill },
  purple: { backgroundColor: theme.chart.purple.fill },
  neutral: { backgroundColor: theme.chart.neutral.fill },
  red: { backgroundColor: theme.chart.red.fill },
  amber: { backgroundColor: theme.chart.amber.fill },
  green: { backgroundColor: theme.chart.green.fill },
})
export const bar = styleVariants({
  /** Both ends are known. */
  ended: [barBase],
  /** The work goes on: the end is open and fades out. */
  ongoing: [
    barBase,
    {
      borderStartEndRadius: 0,
      borderEndEndRadius: 0,
      maskImage: 'linear-gradient(to right, #000 calc(100% - 14px), transparent)',
      minWidth: 16,
    },
  ],
  /** Nothing says when it ended: the end is a dashed edge. */
  unknown: [
    barBase,
    {
      borderStartEndRadius: 0,
      borderEndEndRadius: 0,
      borderInlineEnd: `2px dashed ${theme.content.primary}`,
      minWidth: 8,
    },
  ],
})
/** A thin bar for a record that only frames others, such as a Task. */
export const thinBar = style({ height: 4 })
/** A part of a bar spent waiting: hatched, so it reads apart without colour. */
export const waitBar = style({
  backgroundColor: 'transparent',
  backgroundImage: `repeating-linear-gradient(135deg, ${theme.content.secondary} 0 2px, transparent 2px 5px)`,
  border: `1px solid ${theme.content.secondary}`,
})
export const selectedBar = style({
  outline: `2px solid ${theme.content.primary}`,
  outlineOffset: 1,
})

/**
 * A sign at an end of the track that the bar goes on past it, out of the view. It shows
 * only then: its opacity is 1 when the bar starts before the view, or ends after it, else 0.
 */
const edge = style({
  position: 'absolute',
  insetBlock: 0,
  marginBlock: 'auto',
  height: 'fit-content',
  zIndex: 1,
  display: 'flex',
  pointerEvents: 'none',
  color: theme.content.primary,
  backgroundColor: theme.surface.mainContent,
  borderRadius: 2,
})
/** How far in from the track's start the before-edge sign sits, past a lead there. */
export const edgeInset = createVar()
export const edgeBefore = style([
  edge,
  {
    insetInlineStart: fallbackVar(edgeInset, '0px'),
    opacity: `clamp(0, calc((${viewFrom} - ${itemFrom}) * 1000000), 1)`,
  },
])
export const edgeAfter = style([
  edge,
  {
    insetInlineEnd: 0,
    opacity: `clamp(0, calc((${itemFrom} + ${itemLength} - ${viewFrom} - ${viewSpan}) * 1000000), 1)`,
  },
])

/** What follows a bar's end, such as the sign of a failure. It never shrinks the bar. */
export const after = style({
  position: 'absolute',
  insetBlock: 0,
  left: `max(calc((${itemFrom} + ${itemLength} - ${viewFrom}) / ${viewSpan} * 100%), calc((${itemFrom} - ${viewFrom}) / ${viewSpan} * 100% + 3px))`,
  display: 'flex',
  alignItems: 'center',
  gap: 4,
  paddingInlineStart: 4,
  whiteSpace: 'nowrap',
  pointerEvents: 'none',
  color: theme.content.secondary,
})
/** An open end has no place after it, so its sign sits at the end of the track, over the bar. */
export const afterOpen = style({
  position: 'absolute',
  insetBlock: 0,
  insetInlineEnd: 0,
  marginBlock: 'auto',
  height: 'fit-content',
  // Above a hatched wait drawn over the same bar.
  zIndex: 1,
  display: 'flex',
  alignItems: 'center',
  gap: 4,
  paddingInline: 6,
  borderRadius: 4,
  whiteSpace: 'nowrap',
  pointerEvents: 'none',
  color: theme.content.primary,
  backgroundColor: theme.surface.mainContent,
  border: `1px solid ${theme.stroke.secondary}`,
})

const markBase = style({
  position: 'absolute',
  // Marks remain above a selected or keyboard-focused bar's ring and hit target.
  zIndex: 5,
  insetBlock: 0,
  marginBlock: 'auto',
  left,
  width: 16,
  height: 16,
  // The mark centres on its instant.
  marginInlineStart: -8,
  padding: 0,
  border: 'none',
  background: 'none',
  cursor: 'pointer',
  display: 'grid',
  placeItems: 'center',
  color: theme.content.primary,
  borderRadius: 4,
  selectors: {
    '&::before': { content: '""', boxSizing: 'border-box', backgroundColor: 'currentColor' },
    '&:hover': { color: theme.content.link },
    '&:focus-visible': { outline: `2px solid ${theme.stroke.focused}` },
    '&[aria-current="true"]': {
      outline: `2px solid ${theme.content.primary}`,
      backgroundColor: theme.surface.mainContent,
    },
  },
})
export const mark = styleVariants({
  diamond: [
    markBase,
    { selectors: { '&::before': { width: 8, height: 8, transform: 'rotate(45deg)' } } },
  ],
  circle: [
    markBase,
    {
      selectors: {
        '&::before': {
          width: 8,
          height: 8,
          borderRadius: '50%',
          backgroundColor: theme.surface.mainContent,
          border: '2px solid currentColor',
        },
      },
    },
  ],
  tick: [markBase, { selectors: { '&::before': { width: 2, height: 12 } } }],
  triangle: [
    markBase,
    {
      selectors: {
        '&::before': {
          width: 10,
          height: 9,
          clipPath: 'polygon(50% 0, 100% 100%, 0 100%)',
        },
      },
    },
  ],
})

/** Compressed time in a track of its own: what the band hides, on a plate over the ticks. */
export const fold = style({
  position: 'absolute',
  insetBlock: 0,
  left,
  width,
  minWidth: 14,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  containerType: 'inline-size',
})
export const foldPlate = style({
  display: 'inline-flex',
  alignItems: 'center',
  gap: 2,
  paddingInline: 3,
  borderRadius: 4,
  whiteSpace: 'nowrap',
  color: theme.content.primary,
  backgroundColor: theme.surface.mainContent,
  border: `1px solid ${theme.stroke.secondary}`,
})
/** The band's own text shows only where the band has room for it. */
export const foldText = style({
  display: 'none',
  '@container': { '(min-width: 72px)': { display: 'inline' } },
})

export const axis = style({
  position: 'relative',
  overflow: 'hidden',
  height: '100%',
  minWidth: 0,
  containerType: 'inline-size',
})
export const tick = style({
  position: 'absolute',
  insetBlock: 0,
  display: 'flex',
  alignItems: 'center',
  paddingInline: 4,
  whiteSpace: 'nowrap',
  color: theme.content.tertiary,
  borderInlineStart: `1px solid ${theme.stroke.primary}`,
  selectors: {
    // The last tick reads back from the axis's end, so it stays inside it.
    '&:last-child': {
      borderInlineStart: 'none',
      borderInlineEnd: `1px solid ${theme.stroke.primary}`,
      transform: 'translateX(-100%)',
    },
  },
  // A narrow axis has room for its two ends only.
  '@container': {
    '(max-width: 360px)': {
      selectors: { '&:not(:first-child):not(:last-child)': { display: 'none' } },
    },
  },
})

export const minimap = style({
  position: 'relative',
  height: 28,
  minWidth: 0,
  overflow: 'hidden',
  borderRadius: 4,
  border: `1px solid ${theme.stroke.primary}`,
  backgroundColor: theme.surface.onMainContentSubtle,
  touchAction: 'none',
  cursor: 'pointer',
  vars: { [viewFrom]: '0' },
})
/** How many lanes the strip's spans share, and which one a span is in. */
export const laneCount = createVar()
export const lane = createVar()
/** One interval in the strip: a thin line in its lane, so many show one above the other. */
export const minimapSpan = style({
  position: 'absolute',
  left,
  width,
  minWidth: 2,
  height: 2,
  borderRadius: 1,
  top: `calc((${lane} + 0.5) / ${laneCount} * 100% - 1px)`,
  pointerEvents: 'none',
})
/** How a span ends, by shape, as a bar's end says it. */
export const minimapSpanEnd = styleVariants({
  ended: {},
  /** The work goes on: the end fades out. */
  ongoing: {
    minWidth: 8,
    maskImage: 'linear-gradient(to right, #000 calc(100% - 6px), transparent)',
  },
  /** Nothing says when it ended: a dashed line. */
  unknown: {
    minWidth: 8,
    maskImage: 'repeating-linear-gradient(to right, #000 0 2px, transparent 2px 4px)',
  },
})
/** A span that needs a look: a tick across its line where it starts, so it reads apart without colour. */
export const minimapSpanFlagged = style({
  minWidth: 4,
  selectors: {
    '&::before': {
      content: '""',
      position: 'absolute',
      insetInlineStart: 0,
      top: -3,
      width: 2,
      height: 8,
      borderRadius: 1,
      backgroundColor: 'inherit',
    },
  },
})
export const minimapWindow = style({
  position: 'absolute',
  insetBlock: 0,
  minWidth: 6,
  boxSizing: 'border-box',
  border: `1px solid ${theme.content.primary}`,
  borderRadius: 4,
  backgroundColor: theme.surface.onMainContent,
  cursor: 'grab',
  selectors: {
    '&:focus-visible': { outline: `2px solid ${theme.stroke.focused}`, outlineOffset: -4 },
    '&:active': { cursor: 'grabbing' },
  },
})

/** How wide a lead is, so it ends where its interval starts. */
export const leadWidth = createVar()
/** Where a lead starts: just before its interval, and never before the track. */
const leadLeft = `max(0px, calc((${itemFrom} - ${viewFrom}) / ${viewSpan} * 100% - ${leadWidth}))`
/** What leads an interval on its track: it ends where the interval starts. */
export const lead = style({
  position: 'absolute',
  insetBlock: 0,
  left: leadLeft,
  width: leadWidth,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'flex-end',
  zIndex: 2,
  // A click on the lead reaches the interval and its track, under it, except on a control.
  pointerEvents: 'none',
})
globalStyle(`${lead} :is(button, a, input)`, { pointerEvents: 'auto' })
/** The widest a note grows. It then stays inside the track's width. */
const NOTE_MAX = 360
/** A note about an interval, under its track, from where its lead starts. */
export const note = style({
  position: 'absolute',
  top: '100%',
  left: `clamp(0px, ${leadLeft}, calc(100% - ${NOTE_MAX}px))`,
  maxWidth: NOTE_MAX,
  zIndex: 5,
  marginTop: 2,
  paddingBlock: 2,
  paddingInline: 6,
  borderRadius: 4,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  pointerEvents: 'none',
  backgroundColor: theme.surface.floating,
  border: `1px solid ${theme.stroke.primary}`,
  color: theme.content.primary,
})
