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

/**
 * A duration for a chart's axis and exact readings, from milliseconds: whole milliseconds
 * under a second, hundredths of a second under ten seconds, tenths under a minute, then as
 * `duration` reads.
 */
export function preciseDuration(milliseconds: number) {
  const ms = Math.max(0, milliseconds)
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)}s`
  return duration(ms * 1_000_000)
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

/**
 * The duration of a record that Coral times by its start and end alone, such as a Task. With
 * no recorded end, it says why instead of counting from the clock.
 */
export function RecordedDuration({
  startedAt,
  endedAt,
  ongoing,
}: {
  startedAt: string
  endedAt: string | null
  /** Coral's record says the work goes on. */
  ongoing: boolean
}) {
  if (!endedAt) return ongoing ? 'In progress' : 'No recorded end'
  return duration(Math.max(0, Date.parse(endedAt) - Date.parse(startedAt)) * 1_000_000)
}
