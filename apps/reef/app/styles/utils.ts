import { ComplexStyleRule } from '@vanilla-extract/css'

function boxClamp(lines = 2) {
  return {
    display: '-webkit-box',
    overflow: 'hidden',
    overflowWrap: 'break-word',
    textOverflow: 'ellipsis',
    WebkitBoxOrient: 'vertical',
    WebkitLineClamp: lines,
    wordBreak: 'break-word',
  } satisfies ComplexStyleRule
}

function opacify(colour: string, percentage: number) {
  return `color-mix(in srgb, ${colour} ${percentage}%, transparent)`
}

export const utils = {
  boxClamp,
  opacify,
} as const
