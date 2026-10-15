import { Avatar } from '@/wax/components/avatar'
import { Pill } from '@/wax/components/pill'
import { Typography } from '@/wax/components'
import type { PersonView } from './types'
import * as styles from './management.css'

export function personLabel(person: PersonView) {
  return person.local ? 'Local user' : person.name || person.email || 'Unnamed user'
}
/** A person's label, marked "(You)" for the signed-in user. */
export function personName(person: PersonView, currentUser: string) {
  return `${personLabel(person)}${person.user === currentUser ? ' (You)' : ''}`
}
export function Person({
  person,
  currentUser,
  compact = false,
  disabled = false,
}: {
  person: PersonView
  currentUser: string
  /** One line for table rows: the email moves to a hover title. */
  compact?: boolean
  /** Marks a person whose organization membership is disabled, in words. */
  disabled?: boolean
}) {
  const label = personLabel(person)
  const name = personName(person, currentUser)
  const showEmail = Boolean(person.name && person.email)
  return (
    <div className={styles.row} title={compact && showEmail ? person.email : undefined}>
      <Avatar name={label} seed={person.user} size="20" />
      <div className={styles.intro}>
        <Typography.BodyStrong className={styles.identity}>{name}</Typography.BodyStrong>
        {showEmail && !compact && (
          <Typography.BodySmall className={styles.identity}>{person.email}</Typography.BodySmall>
        )}
      </div>
      {disabled && <Pill size="small">Disabled</Pill>}
    </div>
  )
}
