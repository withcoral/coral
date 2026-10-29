/** Deterministic owning-interface invalidations. No timer drives fixture changes.
 * Every open watch sees each change; a change made while none is open waits for the next.
 */
export function activityChanges() {
  const watches = new Set<ReadableStreamDefaultController<{ changed: boolean }>>()
  const waiting: { changed: boolean }[] = []
  return {
    emit: (changed = true) => {
      if (!watches.size) waiting.push({ changed })
      for (const watch of watches) watch.enqueue({ changed })
    },
    watch: async function* (_input: unknown, context: { signal: AbortSignal }) {
      // A watch aborted before it starts leaves the waiting changes to the next one.
      if (context.signal.aborted) return
      let controller!: ReadableStreamDefaultController<{ changed: boolean }>
      const reader = new ReadableStream<{ changed: boolean }>({
        start(value) {
          controller = value
          for (const change of waiting.splice(0)) value.enqueue(change)
        },
      }).getReader()
      watches.add(controller)
      const abort = () => {
        watches.delete(controller)
        void reader.cancel()
      }
      context.signal.addEventListener('abort', abort, { once: true })
      try {
        yield { changed: true }
        while (!context.signal.aborted) {
          const value = await reader.read()
          if (value.done) return
          yield value.value
        }
      } finally {
        watches.delete(controller)
        context.signal.removeEventListener('abort', abort)
        await reader.cancel()
      }
    },
  }
}
