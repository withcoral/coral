import { utcDay } from '@/lib/activity-list'

// Fixed locale and time zone: the server and every browser render the same text.
const parts = new Intl.DateTimeFormat('en-US', {
  timeZone: 'UTC',
  day: 'numeric',
  month: 'short',
  year: 'numeric',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hourCycle: 'h23',
})

/** A retained instant in UTC, such as "18 Sep 2026, 14:02:00 UTC". The exact stored value
 * stays on the element. A value that is not a time shows as stored. */
export function Instant({ value }: { value: string }) {
  if (!value) return <>Not recorded</>
  const time = Date.parse(value)
  if (Number.isNaN(time)) return <>{value}</>
  const part = Object.fromEntries(parts.formatToParts(time).map((p) => [p.type, p.value]))
  return (
    <time dateTime={value} title={value}>
      {`${part.day} ${part.month} ${part.year}, ${part.hour}:${part.minute}:${part.second} UTC`}
    </time>
  )
}

// A time column reads UTC, as its heading says. An ISO instant splits into the
// UTC time without a time zone lookup.
const utcTime = (instant: string) => instant.slice(11, 19)
const dayParts = new Intl.DateTimeFormat('en-US', {
  timeZone: 'UTC',
  weekday: 'short',
  day: 'numeric',
  month: 'short',
  year: 'numeric',
})
/**
 * Names a UTC calendar day (`YYYY-MM-DD`): "Tue 22 Sep 2026". Never "Today": the
 * reader's today is often another UTC day, and the record alone decides its label.
 */
export function dayLabel(day: string) {
  const part = Object.fromEntries(
    dayParts
      .formatToParts(new Date(`${day}T00:00:00.000Z`))
      .map(({ type, value }) => [type, value]),
  )
  return `${part.weekday} ${part.day} ${part.month} ${part.year}`
}

/** The UTC time of `value` alone, for a column under a day heading. The full instant is
 * on hover. */
export function UtcTime({ value }: { value: string }) {
  return (
    <time dateTime={value} title={`${utcDay(value)} ${utcTime(value)} UTC`}>
      {utcTime(value)}
    </time>
  )
}
