import { useEffect, useState, type ReactNode } from 'react'

/** A local reading aid between server snapshots, never an authority or deadline check. */
export function Elapsed({
  milliseconds,
  running,
  children,
}: {
  milliseconds: number
  running: boolean
  children: (milliseconds: number) => ReactNode
}) {
  const [tick, setTick] = useState({ sample: milliseconds, elapsed: 0 })
  useEffect(() => {
    if (!running) return
    const start = performance.now()
    const timer = setInterval(
      () => setTick({ sample: milliseconds, elapsed: performance.now() - start }),
      1000,
    )
    return () => clearInterval(timer)
  }, [milliseconds, running])
  return children(milliseconds + (running && tick.sample === milliseconds ? tick.elapsed : 0))
}
