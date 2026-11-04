import { Elapsed } from '@/wax/components/elapsed'

/**
 * A duration in the unit that reads best: milliseconds under a second, tenths of a second
 * under ten seconds, whole seconds under a minute, then minutes and seconds, then hours and
 * minutes. Each unit rounds down, so it never shows time that has not passed yet. A counting
 * duration ticks once a second, so it starts in whole seconds. The exact nanoseconds stay with
 * the caller.
 */
export function duration(nanos: string | number, { counting = false } = {}) {
  const ms = Math.max(0, Number(nanos) / 1_000_000)
  if (!counting && ms < 1) return ms === 0 ? '0ms' : '<1ms'
  if (!counting && ms < 1000) return `${Math.floor(ms)}ms`
  if (!counting && ms < 10_000) return `${(Math.floor(ms / 100) / 10).toFixed(1)}s`
  if (ms < 60_000) return `${Math.floor(ms / 1000)}s`
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ${Math.floor(ms / 1000) % 60}s`
  return `${Math.floor(ms / 3_600_000)}h ${Math.floor(ms / 60_000) % 60}m`
}

/** A duration that counts up while its work runs. */
export function ElapsedDuration({
  milliseconds,
  running,
}: {
  milliseconds: number
  running: boolean
}) {
  return (
    <Elapsed milliseconds={milliseconds} running={running}>
      {(value) => duration(value * 1_000_000, { counting: running })}
    </Elapsed>
  )
}

/** Two digits, as a clock shows minutes and seconds. */
const pad = (value: number) => String(value).padStart(2, '0')

/**
 * A duration to the precision a timeline shows: milliseconds under a second, two then one
 * decimal of a second under a minute, then minutes and seconds, then hours and minutes.
 */
export function preciseDuration(ms: number) {
  const value = Math.max(0, ms)
  if (value < 1000) return `${Math.round(value)}ms`
  if (value < 10_000) return `${(Math.floor(value / 10) / 100).toFixed(2)}s`
  if (value < 60_000) return `${(Math.floor(value / 100) / 10).toFixed(1)}s`
  if (value < 3_600_000)
    return `${Math.floor(value / 60_000)}m ${pad(Math.floor(value / 1000) % 60)}s`
  return `${Math.floor(value / 3_600_000)}h ${pad(Math.floor(value / 60_000) % 60)}m`
}

/** How long after a start an instant is, as a time axis labels it. */
export function offset(ms: number) {
  const value = Math.max(0, Math.round(ms))
  if (value === 0) return '0s'
  if (value < 1000) return `${value}ms`
  if (value < 60_000) return `${Number((value / 1000).toFixed(1))}s`
  const seconds = Math.floor(value / 1000)
  if (seconds < 3600) return `${Math.floor(seconds / 60)}:${pad(seconds % 60)}`
  return `${Math.floor(seconds / 3600)}:${pad(Math.floor(seconds / 60) % 60)}:${pad(seconds % 60)}`
}
