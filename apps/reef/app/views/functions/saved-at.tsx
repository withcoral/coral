/** The latest save time in UTC, the same on the server and in the browser. */
export function SavedAt({ at }: { at: string }) {
  return <time dateTime={at}>{`${at.slice(0, 10)} ${at.slice(11, 16)} UTC`}</time>
}
