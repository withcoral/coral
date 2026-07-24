import { style } from '@vanilla-extract/css'

import { baseInput } from '@/wax/components/inputs/base-input.css'
import { theme } from '@/wax/theme/theme.css'

export const panel = style({
  boxSizing: 'border-box',
  display: 'flex',
  flexDirection: 'column',
  height: '100%',
  overflow: 'auto',
  padding: 24,
})

export const tabs = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 24,
  minHeight: 0,
})

export const tabList = style({
  flexShrink: 0,
  paddingInline: 0,
  scrollPaddingInline: 0,
})

export const tabPanel = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 24,
  minHeight: 0,
})

export const panelHeader = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 6,
})

export const manualSection = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
})

const field = style([
  baseInput,
  {
    display: 'flex',
    overflow: 'hidden',
    padding: 0,
    position: 'relative',
    selectors: {
      '&:focus-within': {
        borderColor: theme.input.stroke.focus,
      },
      '&:hover:not(:focus-within)': {
        borderColor: theme.input.stroke.hover,
      },
    },
  },
])

export const promptField = style([
  field,
  {
    flex: 1,
    minHeight: 0,
  },
])

export const commandField = style({
  position: 'relative',
})

export const promptScrollArea = style({
  flex: 1,
  minHeight: 0,
})

export const promptText = style({
  boxSizing: 'border-box',
  color: theme.content.primary,
  flex: 1,
  margin: 0,
  minWidth: 0,
  outline: 'none',
  overflowWrap: 'anywhere',
  paddingBlock: 16,
  paddingInlineEnd: 52,
  paddingInlineStart: 16,
  width: '100%',
  whiteSpace: 'pre-wrap',
  ...theme.typography.code,
})

export const commandInput = style({
  height: 48,
  paddingInlineEnd: 52,
  ...theme.typography.code,
})

export const mcpConfig = style([
  baseInput,
  {
    boxSizing: 'border-box',
    color: theme.content.primary,
    margin: 0,
    paddingBlock: 12,
    paddingInlineEnd: 52,
    paddingInlineStart: 16,
    whiteSpace: 'pre-wrap',
    ...theme.typography.code,
  },
])

export const mcpConfigField = style({
  position: 'relative',
})

export const copyButton = style({
  insetBlockStart: 8,
  insetInlineEnd: 8,
  position: 'absolute',
})
