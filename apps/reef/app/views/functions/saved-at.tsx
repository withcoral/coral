/** The latest save time in UTC, the same on the server and in the browser. An unreadable value shows as given. */
export function SavedAt({ at }: { at: string }) {
  const time = new Date(at)
  if (Number.isNaN(time.getTime())) return <time dateTime={at}>{at}</time>
  const utc = time.toISOString()
  return <time dateTime={at}>{`${utc.slice(0, 10)} ${utc.slice(11, 16)} UTC`}</time>
}
