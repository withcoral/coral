import { expect, it } from 'vitest'
import {
  foldedScale,
  foldsIn,
  intervalGaps,
  intervalsLength,
  unionIntervals,
  wallScale,
} from './scale'

it('joins overlapping and touching intervals and drops empty ones', () => {
  expect(
    unionIntervals([
      { from: 5, to: 8 },
      { from: 0, to: 2 },
      { from: 2, to: 3 },
      { from: 7, to: 9 },
      { from: 4, to: 4 },
    ]),
  ).toEqual([
    { from: 0, to: 3 },
    { from: 5, to: 9 },
  ])
})

it('finds the parts no interval covers, clipped to the range', () => {
  const gaps = intervalGaps({ from: 0, to: 10 }, [
    { from: -5, to: 1 },
    { from: 4, to: 6 },
    { from: 9, to: 20 },
  ])
  expect(gaps).toEqual([
    { from: 1, to: 4 },
    { from: 6, to: 9 },
  ])
  expect(intervalsLength(gaps)).toBe(6)
})

it('keeps wall time proportional and gives a zero-length range a width', () => {
  const scale = wallScale({ from: 100, to: 300 })
  expect(scale.extent).toBe(200)
  expect(scale.toAxis(150)).toBe(50)
  expect(scale.toTime(50)).toBe(150)
  // A division by the extent stays finite.
  const point = wallScale({ from: 100, to: 100 })
  expect(point.extent).toBeGreaterThan(0)
  expect(point.toAxis(100)).toBe(0)
})

it('folds a long gap between busy intervals and maps both ways through it', () => {
  // 100 busy, 10 000 idle, 100 busy.
  const busy = [
    { from: 0, to: 100 },
    { from: 10_100, to: 10_200 },
  ]
  const scale = foldedScale({ from: 0, to: 10_200 }, busy)
  expect(scale.folds).toEqual([{ from: 100, to: 10_100, at: 100, width: 8 }])
  expect(scale.extent).toBe(208)
  // Busy time keeps its length on both sides of the fold.
  expect(scale.toAxis(50)).toBe(50)
  expect(scale.toAxis(10_150)).toBe(158)
  // Time inside the fold stays in order, inside the fold's width.
  expect(scale.toAxis(5_100)).toBe(104)
  for (const time of [0, 50, 100, 5_100, 10_100, 10_200])
    expect(scale.toTime(scale.toAxis(time))).toBeCloseTo(time)
})

it('leaves a short gap unfolded and nothing folded without busy time', () => {
  const short = foldedScale({ from: 0, to: 210 }, [
    { from: 0, to: 100 },
    { from: 110, to: 210 },
  ])
  expect(short.folds).toEqual([])
  expect(short.extent).toBe(210)
  expect(foldedScale({ from: 0, to: 500 }, []).folds).toEqual([])
})

it('folds idle time before the first and after the last busy interval', () => {
  const scale = foldedScale({ from: 0, to: 3_000 }, [{ from: 1_000, to: 2_000 }])
  expect(scale.folds.map(({ from, to }) => [from, to])).toEqual([
    [0, 1_000],
    [2_000, 3_000],
  ])
  expect(scale.extent).toBe(1_080)
  expect(foldsIn(scale, { from: 0, to: 30 })).toHaveLength(1)
  expect(foldsIn(scale, { from: 100, to: 900 })).toHaveLength(0)
})
