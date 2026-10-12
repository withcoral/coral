import classNames from 'classnames'
import { useState } from 'react'

import { IconButton } from '@/wax/components/button'

import * as base from '../base-input.css'
import * as styles from './credential-input.css'

/** Write-only entry. The value stays in the native input, never in React state or props.
 * Submit with a document form, not a router fetcher or enhanced form.
 * The person who types can show the value to check it.
 */
export function CredentialInput({
  id,
  name = 'secret',
  disabled = false,
  required = true,
  masked = true,
}: {
  id: string
  name?: string
  disabled?: boolean
  required?: boolean
  /** Starts the value hidden; an account email, say, can start visible. */
  masked?: boolean
}) {
  const [hidden, setHidden] = useState(masked)
  return (
    <div className={base.container}>
      <input
        id={id}
        name={name}
        type={hidden ? 'password' : 'text'}
        // A provider credential belongs to the provider's site, not this one,
        // so password managers should neither suggest nor offer to save it.
        // Most ignore autocomplete="off" on password fields; the data
        // attributes are their documented opt-outs.
        autoComplete="off"
        data-1p-ignore
        data-lpignore="true"
        data-bwignore
        required={required}
        maxLength={4096}
        disabled={disabled}
        className={classNames(base.input, styles.input)}
      />
      <IconButton
        className={styles.reveal}
        name={hidden ? 'Eye' : 'EyeOff'}
        onClick={() => setHidden(!hidden)}
        disabled={disabled}
        size="22"
        tooltipText={hidden ? 'Show value' : 'Hide value'}
        variant="bare"
      />
    </div>
  )
}
