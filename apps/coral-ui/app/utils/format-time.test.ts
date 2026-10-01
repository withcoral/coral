import { describe, expect, it } from 'vitest'

import { formatDuration, nanosToMs, timeAgo } from './format-time'

const SECOND = 1_000
const MINUTE = 60 * SECOND
const HOUR = 60 * MINUTE
const DAY = 24 * HOUR

const NOW = Date.UTC(2026, 7, 13, 12, 0, 0)

function ago(elapsedMs: number): string {
  return timeAgo(NOW - elapsedMs, NOW)
}

describe('timeAgo', () => {
  it.each([
    [0, 'Just now'],
    [4.9 * SECOND, 'Just now'],
    [5 * SECOND, '5s ago'],
    [42 * SECOND, '42s ago'],
    [59 * SECOND, '59s ago'],
    [MINUTE, '1m ago'],
    [7 * MINUTE, '7m ago'],
    [59 * MINUTE, '59m ago'],
    [HOUR, '1h ago'],
    [3 * HOUR, '3h ago'],
    [23 * HOUR, '23h ago'],
    [DAY, '1d ago'],
    [6 * DAY, '6d ago'],
    [7 * DAY, '1w ago'],
    [21 * DAY, '3w ago'],
    [29 * DAY, '4w ago'],
    [30 * DAY, '1mo ago'],
    [150 * DAY, '5mo ago'],
    [364 * DAY, '11mo ago'],
    [365 * DAY, '1y ago'],
    [729 * DAY, '1y ago'],
    [730 * DAY, '2y ago'],
  ])('renders an age of %dms as %j', (elapsed, expected) => {
    expect(ago(elapsed)).toBe(expected)
  })

  it('never renders a number of weeks that belongs in months', () => {
    for (let days = 7; days < 30; days += 1) {
      expect(ago(days * DAY)).toMatch(/^[1-4]w ago$/)
    }
  })

  it('never renders twelve months', () => {
    for (let days = 30; days < 365; days += 1) {
      expect(ago(days * DAY)).not.toBe('12mo ago')
    }
  })

  it('treats a future timestamp as just now', () => {
    expect(timeAgo(NOW + HOUR, NOW)).toBe('Just now')
  })

  it.each([Number.NaN, Number.POSITIVE_INFINITY])('treats %j as just now', (timestamp) => {
    expect(timeAgo(timestamp, NOW)).toBe('Just now')
  })
})

describe('formatDuration', () => {
  it.each([
    [-1, '—'],
    [Number.NaN, '—'],
    [Number.POSITIVE_INFINITY, '—'],
    [0, '0ms'],
    [0.4, '<1ms'],
    [0.999, '<1ms'],
    [1, '1ms'],
    [842, '842ms'],
    [999, '999ms'],
    // Rounds past the millisecond tier, so it must render as seconds rather than "1000ms".
    [999.6, '1.00s'],
    [1_000, '1.00s'],
    [3_400, '3.40s'],
    [59_499, '59.50s'],
    // Rounds past the seconds tier, so it must render as minutes rather than "60.00s".
    [59_999, '1m'],
    [60_000, '1m'],
    [303_000, '5m 3s'],
    [303_400, '5m 3s'],
    // Rounds past the minutes tier, so it must render as hours rather than "60m".
    [3_599_999, '1h'],
    [3_600_000, '1h'],
    [8_040_000, '2h 14m'],
    [86_400_000, '24h'],
  ])('renders %dms as %j', (ms, expected) => {
    expect(formatDuration(ms)).toBe(expected)
  })

  it('never renders a sixty-second or sixty-minute component', () => {
    // Anchored on a component boundary so "2.60s" is not mistaken for a 60 second value.
    const sixty = /(?:^|\s)60(?:\.\d+)?[sm]\b/
    for (let ms = 0; ms < 4 * HOUR; ms += 137) {
      expect(formatDuration(ms)).not.toMatch(sixty)
    }
  })
})

describe('nanosToMs', () => {
  it('keeps sub-millisecond spans instead of truncating them to zero', () => {
    expect(nanosToMs('500000')).toBe(0.5)
    expect(formatDuration(nanosToMs('500000'))).toBe('<1ms')
  })

  it('converts a realistic span duration', () => {
    expect(nanosToMs('1234567890')).toBeCloseTo(1234.56789, 5)
  })

  it('converts epoch nanos to a usable millisecond timestamp', () => {
    expect(nanosToMs('1755086400000000000')).toBe(1_755_086_400_000)
  })

  it('accepts bigint and empty input', () => {
    expect(nanosToMs(2_000_000n)).toBe(2)
    expect(nanosToMs('')).toBe(0)
  })
})
