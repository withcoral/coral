import * as Button from '@/wax/components/button'
import { TextInput } from '@/wax/components/inputs/text/text-input'
import { Typography } from '@/wax/components/typography'
import * as styles from './find-controls.css'

/**
 * A viewer's Find: the field, what it found, and the previous and next match. Enter steps
 * to the next match from the field, and Shift+Enter to the previous one.
 */
export function FindControls({
  label,
  query,
  onQueryChange,
  status,
  count,
  onStep,
}: {
  /** What Find searches; the field is named "Find in" it. */
  label: string
  query: string
  onQueryChange: (next: string) => void
  /** What Find says it found, such as "1 of 3". */
  status: string
  /** How many matches there are to step through. */
  count: number
  onStep: (by: 1 | -1) => void
}) {
  return (
    <div className={styles.find}>
      <div className={styles.input}>
        <TextInput
          type="search"
          icon="Search"
          ariaLabel={`Find in ${label}`}
          placeholder="Find"
          value={query}
          onChange={onQueryChange}
          onKeyDown={(event) => {
            if (event.key !== 'Enter') return
            event.preventDefault()
            onStep(event.shiftKey ? -1 : 1)
          }}
        />
      </div>
      <Typography.BodySmall variant="tertiary" role="status">
        {status}
      </Typography.BodySmall>
      <Button.IconButton
        name="ChevronUp"
        size="22"
        variant="bare"
        ariaLabel="Previous match"
        disabled={!count}
        onClick={() => onStep(-1)}
      />
      <Button.IconButton
        name="ChevronDown"
        size="22"
        variant="bare"
        ariaLabel="Next match"
        disabled={!count}
        onClick={() => onStep(1)}
      />
    </div>
  )
}
