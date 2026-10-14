import { useEffect, useState } from 'react'
import { Typography } from './typography'

const plural = (count: number, unit: string) => `${count} ${unit}${count === 1 ? '' : 's'}`

/** A reading aid only: the server, not the browser clock, decides action availability. */
export function Deadline({
  at,
  variant,
}: {
  at: string
  /** Text colour, for a deadline that sits in a line of secondary metadata. */
  variant?: 'secondary' | 'tertiary'
}) {
  const [now, setNow] = useState<number>()
  const remaining = now === undefined ? undefined : Date.parse(at) - now
  useEffect(() => {
    if (remaining !== undefined && remaining <= 0) return
    // Wake only when the label changes: each minute, then each second in the last minute.
    const delay =
      remaining === undefined ? 0 : ((remaining - 1) % (remaining > 60_000 ? 60_000 : 1000)) + 1
    const timer = setTimeout(() => setNow(Date.now()), delay)
    return () => clearTimeout(timer)
  }, [remaining])
  const label =
    remaining === undefined
      ? `Deadline: ${at}`
      : remaining <= 0
        ? 'Deadline reached. Check the current state.'
        : remaining <= 60_000
          ? `Expires in ${plural(Math.ceil(remaining / 1000), 'second')}`
          : `Expires in ${plural(Math.ceil(remaining / 60_000), 'minute')}`
  return (
    <Typography.BodySmall as="time" dateTime={at} title={at} variant={variant}>
      {label}
    </Typography.BodySmall>
  )
}
