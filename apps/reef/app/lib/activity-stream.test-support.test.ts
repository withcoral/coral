import { expect, it } from 'vitest'
import { activityChanges } from './activity-stream.test-support'

// Catches: a change made just after a watch was aborted, before its stream closed,
// failing the test that made it instead of reaching the watch that reconnects.
it('passes a change made after an abort to the next watch', async () => {
  const changes = activityChanges()
  const aborted = new AbortController()
  const first = changes.watch({}, { signal: aborted.signal })
  expect((await first.next()).value).toEqual({ changed: true })
  aborted.abort()
  expect(() => changes.emit(false)).not.toThrow()
  await first.return(undefined)

  const next = changes.watch({}, { signal: new AbortController().signal })
  expect((await next.next()).value).toEqual({ changed: true })
  expect((await next.next()).value).toEqual({ changed: false })
  await next.return(undefined)
})

// Catches: a watch that was aborted before it started taking the changes that wait for
// the next watch, so the watch that reconnects never sees them.
it('leaves waiting changes to the next watch when a watch starts aborted', async () => {
  const changes = activityChanges()
  changes.emit(false)
  const aborted = new AbortController()
  aborted.abort()
  const first = changes.watch({}, { signal: aborted.signal })
  await first.next()
  await first.return(undefined)

  const next = changes.watch({}, { signal: new AbortController().signal })
  expect((await next.next()).value).toEqual({ changed: true })
  expect((await next.next()).value).toEqual({ changed: false })
  await next.return(undefined)
})
