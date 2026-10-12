/** Deterministic owning-interface invalidations. No timer drives fixture changes. */
export function activityChanges() {
  let controller!: ReadableStreamDefaultController<{ changed: boolean }>
  const stream = new ReadableStream<{ changed: boolean }>({
    start(value) {
      controller = value
    },
  })
  return {
    emit: (changed = true) => controller.enqueue({ changed }),
    watch: async function* (_input: unknown, context: { signal: AbortSignal }) {
      const reader = stream.getReader()
      const abort = () => {
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
        context.signal.removeEventListener('abort', abort)
        await reader.cancel()
      }
    },
  }
}
