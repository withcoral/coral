import { describe, expect, it } from 'vitest'
import { foldedTicks, foldTime } from './fold'

const bounds = { from: 0, to: 100 }

describe('foldTime', () => {
  // Catches: a long gap that still takes the axis, so the work around it stays a hairline.
  it('folds a long gap to a short break and keeps the time around it to scale', () => {
    // 20ms of work and an 80ms gap: the break takes a fifth of the folded axis.
    const fold = foldTime(bounds, [{ from: 10, to: 90 }], 0.2)
    expect(fold.bounds).toEqual({ from: 0, to: 25 })
    expect(fold.breaks).toEqual([{ real: { from: 10, to: 90 }, folded: { from: 10, to: 15 } }])
    expect([0, 10, 50, 90, 100].map(fold.fold)).toEqual([0, 10, 12.5, 15, 25])
    expect([0, 10, 12.5, 15, 25].map(fold.unfold)).toEqual([0, 10, 50, 90, 100])
  })

  // Catches: a short gap widened by folding, breaks that crowd out the work, an axis folded to
  // nothing, or a running bar that stops at the axis end.
  it.each([
    {
      label: 'keeps a gap shorter than a break to scale',
      gaps: [
        { from: 10, to: 90 },
        { from: 95, to: 96 },
      ],
      share: 0.2,
      breaks: 1,
      at: [96, 100],
      folded: [21, 25],
    },
    {
      label: 'gives many breaks a quarter of the axis at most',
      gaps: [
        { from: 10, to: 45 },
        { from: 55, to: 90 },
      ],
      // Asked for 40% each, two breaks get 12.5% each: 30ms of work keeps three quarters.
      share: 0.4,
      breaks: 2,
      at: [10, 45, 55, 90, 100],
      folded: [10, 15, 25, 30, 40],
    },
    {
      label: 'leaves an axis that is all gap unfolded',
      gaps: [{ from: 0, to: 100 }],
      share: 0.2,
      breaks: 0,
      at: [0, 50, 100],
      folded: [0, 50, 100],
    },
    {
      label: 'keeps the scale past the end, for work that goes on',
      gaps: [{ from: 10, to: 90 }],
      share: 0.2,
      breaks: 1,
      at: [110],
      folded: [35],
    },
  ])('$label', ({ gaps, share, breaks, at, folded }) => {
    const fold = foldTime(bounds, gaps, share)
    expect(fold.breaks).toHaveLength(breaks)
    expect(at.map(fold.fold)).toEqual(folded)
    expect(folded.map(fold.unfold)).toEqual(at)
  })
})

describe('foldedTicks', () => {
  // Catches: ticks at folded instants no reader counts in, or two labels on top of each other
  // either side of a break.
  it('labels round real times in the time around each break, apart enough to read', () => {
    const fold = foldTime(bounds, [{ from: 10, to: 90 }], 0.2)
    // 250px for 25 folded ms: each stretch of 10ms takes 100px, the break 50px. A label runs
    // right from its tick, so the one at 10ms, 25px before the break's zigzag, would cross it.
    const ticks = foldedTicks(fold, fold.bounds, bounds.from, 250)
    expect(ticks.map(fold.unfold)).toEqual([0, 90, 100])
  })

  // Catches: a tick label drawn over a break's zigzag, or a stretch that loses the label
  // just after its break, where a label runs clear of the zigzag.
  it('labels the time just after a break, not just before it', () => {
    const fold = foldTime(bounds, [{ from: 10, to: 90 }], 0.2)
    // 100px for 25 folded ms: 90ms falls 10px after the break's middle.
    const ticks = foldedTicks(fold, fold.bounds, bounds.from, 100)
    expect(ticks.map(fold.unfold)).toEqual([0, 90])
  })

  // Catches: an axis with no start label when the root opens with a gap.
  it('labels the start even when a break opens the axis', () => {
    const fold = foldTime(bounds, [{ from: 0, to: 80 }], 0.2)
    const ticks = foldedTicks(fold, fold.bounds, bounds.from, 250)
    expect(ticks.map(fold.unfold)[0]).toBe(0)
  })
})
