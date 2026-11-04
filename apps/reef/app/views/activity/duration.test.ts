import { describe, expect, it } from 'vitest'
import { duration, offset, preciseDuration } from './duration'

describe('preciseDuration', () => {
  // Catches: bar durations that round a short call to 0s or show time not yet passed.
  it.each([
    [0, '0ms'],
    [182, '182ms'],
    [2370, '2.37s'],
    [17_640, '17.6s'],
    [186_000, '3m 06s'],
    [3_900_000, '1h 05m'],
  ])('reads %i ms as %s', (ms, text) => {
    expect(preciseDuration(ms)).toBe(text)
  })
})

describe('offset', () => {
  // Catches: axis labels that do not read as time after the root's start.
  it.each([
    [0, '0s'],
    [250, '250ms'],
    [12_500, '12.5s'],
    [30_000, '30s'],
    [60_000, '1:00'],
    [245_000, '4:05'],
    [3_725_000, '1:02:05'],
  ])('labels %i ms after the start as %s', (ms, text) => {
    expect(offset(ms)).toBe(text)
  })
})

describe('duration', () => {
  // Catches: a short call that reads "0s", a unit that shows time that has not passed yet, or
  // a counting duration whose last digit sits still between its one-second ticks.
  it.each([
    [0, '0ms', '0s'],
    [400_000, '<1ms', '0s'],
    [263_900_000, '263ms', '0s'],
    [999_999_999, '999ms', '0s'],
    [1_000_000_000, '1.0s', '1s'],
    [2_490_000_000, '2.4s', '2s'],
    [9_999_999_999, '9.9s', '9s'],
    [10_000_000_000, '10s', '10s'],
    [59_999_999_999, '59s', '59s'],
    [263_001_000_000, '4m 23s', '4m 23s'],
    [3_600_000_000_000, '1h 0m', '1h 0m'],
  ])('shows %d ns as %s, or %s while it counts', (nanos, ended, counting) => {
    expect(duration(nanos)).toBe(ended)
    expect(duration(String(nanos))).toBe(ended)
    expect(duration(nanos, { counting: true })).toBe(counting)
  })
})
