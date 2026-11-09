/** The latest save time in UTC, the same on the server and in the browser. */
export function SavedAt({ at }: { at: string }) {
  const utc = toUtc(at)
  return <time dateTime={at}>{`${utc.slice(0, 10)} ${utc.slice(11, 16)} UTC`}</time>
}

/** An RFC 3339 time with any offset, moved to UTC. An unreadable value shows as given. */
function toUtc(at: string): string {
  const time = new Date(at)
  return Number.isNaN(time.getTime()) ? at : time.toISOString()
}
