import * as React from 'react'

export function useNow({
  refreshAfterMs,
  refreshOnTheHour,
  seedMs,
}: {
  refreshAfterMs?: number
  refreshOnTheHour?: boolean
  /** The timestamp the server rendered with. Seeding from it keeps hydration consistent. */
  seedMs: number
}) {
  const [tickedAtMs, setTickedAtMs] = React.useState(0)

  const refresh = React.useCallback(() => {
    setTickedAtMs((previous) => Math.max(previous, Date.now()))
  }, [])

  // The later of the two, so a fresh seed is picked up even when nothing is ticking, and
  // a client clock behind the server cannot run the result backwards.
  const now = new Date(Math.max(seedMs, tickedAtMs))

  React.useEffect(() => {
    if (!refreshAfterMs) return

    const interval = setInterval(refresh, refreshAfterMs)

    return () => clearInterval(interval)
  }, [refresh, refreshAfterMs])

  React.useEffect(() => {
    if (!refreshOnTheHour) return

    const getMsUntilNextHour = () => {
      const current = new Date()
      const next = new Date(current)
      next.setHours(next.getHours() + 1)
      next.setMinutes(0)
      next.setSeconds(0)
      next.setMilliseconds(0)

      return Number(next) - Number(current)
    }

    // using individual timeouts instead of an interval allows us to calculate
    // the time until the next hour on every iteration and avoid drift over time
    let timeout: ReturnType<typeof setTimeout>
    const interval = () => {
      refresh()
      timeout = setTimeout(interval, getMsUntilNextHour())
    }

    timeout = setTimeout(interval, getMsUntilNextHour())

    return () => clearTimeout(timeout)
  }, [refresh, refreshOnTheHour])

  return { now, refresh }
}
