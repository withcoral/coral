import { Avatar } from '@/wax/components/avatar'
import { Pill } from '@/wax/components/pill'
import { Typography } from '@/wax/components'
import type { PersonView } from './types'
import * as styles from './management.css'

export function personLabel(person: PersonView) {
  return person.local ? 'Local user' : person.name || person.email || 'Unnamed user'
}
/** A person's label, or the given one, marked "(You)" for the signed-in user. */
export function personName(person: PersonView, currentUser: string, label = personLabel(person)) {
  return `${label}${person.user === currentUser ? ' (You)' : ''}`
}
export function Person({
  person,
  currentUser,
  compact = false,
  disabled = false,
  emailColumn = false,
}: {
  person: PersonView
  currentUser: string
  /** One line for table rows: the name truncates, and the email moves to a hover title. */
  compact?: boolean
  /** Marks a person whose organization membership is disabled, in words. */
  disabled?: boolean
  /** The table shows the email in its own column, so the name never falls back to it. */
  emailColumn?: boolean
}) {
  const label = personLabel(person)
  const name = personName(
    person,
    currentUser,
    emailColumn && !person.local && !person.name ? '(no name)' : undefined,
  )
  const showEmail = !emailColumn && Boolean(person.name && person.email)
  if (compact) {
    // The full name and the email show on hover, since a narrow column truncates the name.
    return (
      <div className={styles.personLine} title={showEmail ? `${name}\n${person.email}` : name}>
        <Avatar name={label} seed={person.user} size="20" />
        <Typography.BodyStrong className={styles.personLineName} truncate>
          {name}
        </Typography.BodyStrong>
        {disabled && <Pill size="small">Disabled</Pill>}
      </div>
    )
  }
  return (
    <div className={styles.row}>
      <Avatar name={label} seed={person.user} size="20" />
      <div className={styles.intro}>
        <Typography.BodyStrong className={styles.identity}>{name}</Typography.BodyStrong>
        {showEmail && (
          <Typography.BodySmall className={styles.identity}>{person.email}</Typography.BodySmall>
        )}
      </div>
      {disabled && <Pill size="small">Disabled</Pill>}
    </div>
  )
}
