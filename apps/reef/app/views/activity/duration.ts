/**
 * A duration in whole elapsed time: seconds under a minute, then minutes and seconds, then hours
 * and minutes. Each unit rounds down, so it never shows time that has not passed yet. The exact
 * nanoseconds stay with the caller.
 */
export function duration(nanos: string | number) {
  const ms = Math.max(0, Number(nanos) / 1_000_000)
  if (ms < 60_000) return `${Math.floor(ms / 1000)}s`
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ${Math.floor(ms / 1000) % 60}s`
  return `${Math.floor(ms / 3_600_000)}h ${Math.floor(ms / 60_000) % 60}m`
}
