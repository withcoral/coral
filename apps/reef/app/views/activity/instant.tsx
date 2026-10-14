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
