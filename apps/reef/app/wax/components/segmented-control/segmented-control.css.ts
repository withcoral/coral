import { globalStyle, recipe, style } from '@/wax/css'

import { animation, theme } from '@/wax/theme/theme.css'

export const group = style({
  background: theme.surface.onMainContent,
  borderRadius: 8,
  display: 'inline-flex',
  gap: 2,
  maxWidth: '100%',
  overflowX: 'auto',
  padding: 2,
})
export const segment = recipe({
  base: {
    background: 'transparent',
    border: 0,
    borderRadius: 6,
    color: theme.content.tertiary,
    cursor: 'pointer',
    flexShrink: 0,
    minHeight: 30,
    paddingBlock: 4,
    paddingInline: 12,
    transition: animation.colorTransition,
    whiteSpace: 'nowrap',
    ...theme.typography.buttonStrong,
    selectors: {
      '&:hover:not(:disabled)': { color: theme.content.primary },
      '&:focus-visible': { outline: `2px solid ${theme.stroke.focused}`, outlineOffset: -2 },
      '&:disabled': { color: theme.content.disabled, cursor: 'not-allowed' },
    },
  },
  variants: {
    checked: {
      // Raised off the track, so the choice reads at a glance.
      true: {
        background: theme.segmentedControl.checked,
        boxShadow: `inset 0 0 0 1px ${theme.segmentedControl.checkedStroke}, ${theme.elevation.e1}`,
        color: theme.content.primary,
      },
      false: {},
    },
    // A square segment that holds one icon.
    icon: {
      true: {
        alignItems: 'center',
        display: 'inline-flex',
        justifyContent: 'center',
        minHeight: 30,
        minWidth: 32,
        paddingBlock: 0,
        paddingInline: 0,
      },
      false: {},
    },
    size: { default: {}, small: {} },
  },
  compoundVariants: [
    // A small control fits inside a compact table row.
    {
      variants: { icon: false, size: 'small' },
      style: { minHeight: 24, paddingBlock: 2, paddingInline: 8 },
    },
    {
      variants: { icon: true, size: 'small' },
      style: { minHeight: 24, minWidth: 26 },
    },
    // A chosen icon repeats down a table, so it recedes; the raised chip shows the choice.
    {
      variants: { checked: true, icon: true },
      style: { color: theme.content.muted },
    },
    // A read-only control still shows its choice.
    {
      variants: { checked: true },
      style: { selectors: { '&:disabled': { color: theme.content.secondary } } },
    },
  ],
})

// Icons come from sets drawn on different grids. One screen-pixel stroke
// makes the segments read as one set.
export const icon = style({})
globalStyle(`${icon} svg, ${icon} svg *`, {
  strokeWidth: '1.5px',
  vectorEffect: 'non-scaling-stroke',
})

export const tooltipLabel = style({
  color: theme.content.primary,
  display: 'block',
  ...theme.typography.bodySmallStrong,
})
export const tooltipDescription = style({ display: 'block' })
