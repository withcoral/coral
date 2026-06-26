import { globalStyle, style } from '@vanilla-extract/css'

import { animation, theme } from '@/wax/theme/theme.css'

export const calendar = style({
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
  width: '252px',
})

export const header = style({
  alignItems: 'center',
  display: 'flex',
  gap: '4px',
  justifyContent: 'space-between',
  paddingBlock: '8px',
  paddingInline: '6px',
})

export const heading = style({
  color: theme.content.secondary,
  flex: 1,
  margin: 0,
  textAlign: 'center',
  ...theme.typography.buttonStrong,
})

export const calendarGrid = style({
  borderCollapse: 'collapse',
  width: '100%',
})

export const calendarCell = style({
  alignItems: 'center',
  backgroundColor: 'transparent',
  border: 'none',
  borderRadius: '8px',
  color: theme.content.secondary,
  cursor: 'pointer',
  display: 'flex',
  height: '32px',
  justifyContent: 'center',
  outline: 'none',
  selectors: {
    '&:focus-visible': {
      backgroundColor: theme.surface.onMainContent,
    },
    '&:hover': {
      backgroundColor: theme.surface.onMainContent,
    },
    '&[data-disabled]': {
      color: theme.content.disabled,
      cursor: 'not-allowed',
    },
    '&[data-outside-month]': {
      visibility: 'hidden',
    },
    '&[data-selected]': {
      backgroundColor: theme.button.primary.default,
      color: theme.content.accentContent.primaryReverse,
    },
    '&[data-selected]:hover': {
      backgroundColor: theme.button.primary.hover,
    },
    '&[data-unavailable]': {
      color: theme.content.disabled,
      cursor: 'not-allowed',
      textDecoration: 'line-through',
    },
  },
  transition: animation.colorTransition,
  width: '32px',
})

export const label = style({
  paddingInline: '10px 2px',
})

globalStyle(`${calendar} .react-aria-CalendarHeaderCell`, {
  color: theme.content.tertiary,
  ...theme.typography.buttonStrong,
})

globalStyle(`${calendarGrid} th:first-child, ${calendarGrid} td:first-child`, {
  paddingInlineStart: '8px',
})

globalStyle(`${calendarGrid} th:last-child, ${calendarGrid} td:last-child`, {
  paddingInlineEnd: '8px',
})
