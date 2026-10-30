import { useId, useRef, useState } from 'react'
import { useLocation, useNavigate } from 'react-router'
import { Inputs, Typography } from '@/wax/components'
import {
  ACTIVITY_SEARCH_FIELD_CHARS,
  ACTIVITY_SEARCH_MIN_CHARS,
  activitySearchKey,
} from '@/lib/activity-search'
import * as styles from './task-activity.css'

/**
 * Searches Task and Run intent as you type. The address keeps the search, so a reload
 * or Back shows it again. Each search you start adds one history entry; the text you
 * type after it replaces that entry.
 */
export function ActivitySearch({ searchHref }: { searchHref: (typed: string) => string }) {
  const navigate = useNavigate()
  const { search } = useLocation()
  const addressed = new URLSearchParams(search).get(activitySearchKey) ?? ''
  const [typed, setTyped] = useState(addressed)
  // The text this field last sent, and the address it last saw. An address that
  // changes to other text, such as on Back, replaces the text. A navigation still
  // under way leaves the address as it was.
  const [sent, setSent] = useState(addressed)
  const [seen, setSeen] = useState(addressed)
  if (addressed !== seen) {
    setSeen(addressed)
    if (addressed !== sent) {
      setSent(addressed)
      setTyped(addressed)
    }
  }
  const typing = useRef(false)
  const hintId = useId()
  const length = [...typed.trim()].length
  const short = length > 0 && length < ACTIVITY_SEARCH_MIN_CHARS
  return (
    <div className={styles.search}>
      <Inputs.TextInput
        type="search"
        ariaLabel="Search intent"
        placeholder="Search intent"
        ariaDescribedBy={short ? hintId : undefined}
        maxLength={ACTIVITY_SEARCH_FIELD_CHARS}
        value={typed}
        onChange={(value) => {
          setTyped(value)
          const next = value.trim() ? value : ''
          setSent(next)
          void navigate(searchHref(next), {
            replace: typing.current,
            preventScrollReset: true,
          })
          typing.current = true
        }}
        onBlur={() => {
          typing.current = false
        }}
      />
      {short && (
        <Typography.BodySmall id={hintId} role="status">
          Type at least {ACTIVITY_SEARCH_MIN_CHARS} characters to search.
        </Typography.BodySmall>
      )}
    </div>
  )
}
