const SECOND_MS = 1_000
const MINUTE_MS = 60 * SECOND_MS
const HOUR_MS = 60 * MINUTE_MS
const DAY_MS = 24 * HOUR_MS
const WEEK_MS = 7 * DAY_MS

const DAYS_PER_WEEK = 7
const DAYS_PER_MONTH = 30
const DAYS_PER_YEAR = 365
const MONTHS_PER_YEAR = 12

/** Anything more recent than this reads as "Just now" rather than a second count. */
const JUST_NOW_MS = 5 * SECOND_MS

export function nanosToMs(nanos: string | bigint | number): number {
  const value = typeof nanos === 'bigint' ? nanos : BigInt(nanos || 0)
  return Number(value) / 1_000_000
}

/**
 * Formats a millisecond span, picking the coarsest unit that keeps the number small.
 *
 * @example
 * formatDuration(0) // "0ms"
 * formatDuration(0.4) // "<1ms"
 * formatDuration(842) // "842ms"
 * formatDuration(3_400) // "3.40s"
 * formatDuration(303_000) // "5m 3s"
 * formatDuration(8_040_000) // "2h 14m"
 */
export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return '—'
  if (ms === 0) return '0ms'
  if (ms < 1) return '<1ms'

  // Round to display precision before choosing a tier, so a value that rounds up past a
  // boundary lands in the next tier instead of rendering "1000ms" or "5m 60s".
  const milliseconds = Math.round(ms)
  if (milliseconds < SECOND_MS) return `${milliseconds}ms`

  const seconds = Math.round(ms / 10) / 100
  if (seconds < 60) return `${seconds.toFixed(2)}s`

  const wholeSeconds = Math.round(ms / SECOND_MS)
  if (wholeSeconds < 3_600) {
    const minutes = Math.floor(wholeSeconds / 60)
    const remainder = wholeSeconds % 60
    return remainder === 0 ? `${minutes}m` : `${minutes}m ${remainder}s`
  }

  const wholeMinutes = Math.round(ms / MINUTE_MS)
  const hours = Math.floor(wholeMinutes / 60)
  const remainder = wholeMinutes % 60
  return remainder === 0 ? `${hours}h` : `${hours}h ${remainder}m`
}

export function formatDurationFromNanos(nanos: string): string {
  return formatDuration(nanosToMs(nanos))
}

export function formatTimestamp(timestamp: number): string {
  if (!Number.isFinite(timestamp) || timestamp <= 0) return 'Unknown time'
  return new Date(timestamp).toLocaleString(undefined, {
    dateStyle: 'medium',
    timeStyle: 'medium',
  })
}

/**
 * Formats how long ago `timestamp` was, rolling up through weeks, months and years so the
 * number stays small. Months and years are approximate rather than calendar-correct.
 *
 * A timestamp in the future reads as "Just now", so a client clock running behind the
 * server never renders a negative age.
 *
 * @example
 * timeAgo(now - 42_000, now) // "42s ago"
 * timeAgo(now - 6 * DAY_MS, now) // "6d ago"
 * timeAgo(now - 21 * DAY_MS, now) // "3w ago"
 */
export function timeAgo(timestamp: number, referenceTimeMs: number): string {
  const elapsed = referenceTimeMs - timestamp
  if (!Number.isFinite(elapsed) || elapsed < JUST_NOW_MS) return 'Just now'
  if (elapsed < MINUTE_MS) return `${Math.floor(elapsed / SECOND_MS)}s ago`
  if (elapsed < HOUR_MS) return `${Math.floor(elapsed / MINUTE_MS)}m ago`
  if (elapsed < DAY_MS) return `${Math.floor(elapsed / HOUR_MS)}h ago`
  if (elapsed < WEEK_MS) return `${Math.floor(elapsed / DAY_MS)}d ago`

  const days = Math.floor(elapsed / DAY_MS)
  if (days < DAYS_PER_MONTH) return `${Math.floor(days / DAYS_PER_WEEK)}w ago`
  if (days < DAYS_PER_YEAR) {
    // Clamp so the last few days of the year cannot render as "12mo ago".
    return `${Math.min(MONTHS_PER_YEAR - 1, Math.floor(days / DAYS_PER_MONTH))}mo ago`
  }
  return `${Math.floor(days / DAYS_PER_YEAR)}y ago`
}
