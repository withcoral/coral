import { useState } from 'react'
import * as styles from './number-pop-in.css'

const STAGGER_MS = 70

/**
 * Text whose changed characters pop in when its value changes. Characters the
 * old and new values share at the start and end stay still, so "+1.2 kB" to
 * "+1.5 kB" pops only the "5". The first render shows the value at rest, so
 * only live updates draw the eye.
 */
export function NumberPopIn({ value }: { value: string }) {
  const [shown, setShown] = useState({ value, previous: null as string | null, version: 0 })
  if (shown.value !== value) setShown({ value, previous: shown.value, version: shown.version + 1 })
  if (shown.previous === null) return value
  const next = [...value]
  const before = [...shown.previous]
  let start = 0
  while (start < next.length && start < before.length && next[start] === before[start]) start++
  let end = 0
  while (
    end < next.length - start &&
    end < before.length - start &&
    next[next.length - 1 - end] === before[before.length - 1 - end]
  )
    end++
  const stop = next.length - end
  return (
    <span className={styles.group}>
      {next.map((character, index) =>
        index < start || index >= stop ? (
          <span key={index < start ? `start-${index}` : `end-${next.length - index}`}>
            {character}
          </span>
        ) : (
          // A new key per change remounts the character, which replays the animation.
          <span
            key={`${shown.version}-${index}`}
            className={styles.character}
            style={{ animationDelay: `${(index - start) * STAGGER_MS}ms` }}
          >
            {character}
          </span>
        ),
      )}
    </span>
  )
}
