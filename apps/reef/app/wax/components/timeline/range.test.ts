import { describe, expect, it } from 'vitest'
import { focus, pan, timeTicks, zoomAt } from './range'

const bounds = { from: 0, to: 100 }

describe('zoomAt', () => {
  // Catches: zoom that drifts the point under the pointer.
  it('keeps the instant under the pointer where it is', () => {
    expect(zoomAt({ from: 0, to: 100 }, bounds, 0.25, 0.5)).toEqual({ from: 12.5, to: 62.5 })
  })

  // Catches: zoom past the root's bounds, or narrower than a timestamp can show.
  it.each([
    {
      label: 'stops zooming out at the bounds',
      range: { from: 0, to: 100 },
      at: 0.5,
      factor: 4,
      expected: { from: 0, to: 100 },
    },
    {
      label: 'slides back inside the bounds',
      range: { from: 80, to: 100 },
      at: 0.9,
      factor: 2,
      expected: { from: 60, to: 100 },
    },
    {
      label: 'stops zooming in at its least width',
      range: { from: 40, to: 42 },
      at: 0.5,
      factor: 0.01,
      expected: { from: 40.5, to: 41.5 },
    },
  ])('$label', ({ range, at, factor, expected }) => {
    expect(zoomAt(range, bounds, at, factor)).toEqual(expected)
  })
})

describe('pan', () => {
  // Catches: panning off either end of the root.
  it.each([
    {
      label: 'moves by a share of its width',
      range: { from: 0, to: 10 },
      by: 0.5,
      expected: { from: 5, to: 15 },
    },
    {
      label: 'stops at the end',
      range: { from: 90, to: 100 },
      by: 1,
      expected: { from: 90, to: 100 },
    },
    {
      label: 'stops at the start',
      range: { from: 5, to: 15 },
      by: -1,
      expected: { from: 0, to: 10 },
    },
  ])('$label', ({ range, by, expected }) => {
    expect(pan(range, bounds, by)).toEqual(expected)
  })
})

describe('focus', () => {
  // Catches: zoom to selection cutting off its record, leaving the bounds, or dividing by an
  // instant's zero width.
  it.each([
    {
      label: 'frames an interval with a margin',
      interval: { from: 40, to: 60 },
      expected: { from: 38, to: 62 },
    },
    {
      label: 'keeps the frame inside the bounds',
      interval: { from: 0, to: 50 },
      expected: { from: 0, to: 55 },
    },
    {
      label: 'gives an instant its least width',
      interval: { from: 30, to: 30 },
      expected: { from: 29.5, to: 30.5 },
    },
  ])('$label', ({ interval, expected }) => {
    expect(focus(interval, bounds)).toEqual(expected)
  })
})

describe('timeTicks', () => {
  // Catches: ticks on steps a reader does not count in, or not counted from the root's start.
  it.each([
    {
      label: 'steps through a minute in tens of seconds',
      range: { from: 0, to: 60_000 },
      origin: 0,
      count: 6,
      expected: [0, 10_000, 20_000, 30_000, 40_000, 50_000, 60_000],
    },
    {
      label: 'steps through half a second in tenths',
      range: { from: 1000, to: 1500 },
      origin: 0,
      count: 5,
      expected: [1000, 1100, 1200, 1300, 1400, 1500],
    },
    {
      label: 'steps through minutes in whole minutes',
      range: { from: 5000, to: 400_000 },
      origin: 0,
      count: 8,
      expected: [60_000, 120_000, 180_000, 240_000, 300_000, 360_000],
    },
    {
      label: 'counts from its origin',
      range: { from: 1003, to: 1033 },
      origin: 1003,
      count: 3,
      expected: [1003, 1013, 1023, 1033],
    },
  ])('$label', ({ range, origin, count, expected }) => {
    expect(timeTicks(range, origin, count)).toEqual(expected)
  })
})
