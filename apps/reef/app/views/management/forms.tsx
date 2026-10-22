import { createContext, useEffect, useId, useRef, useState } from 'react'
import { useFetcher } from 'react-router'

import { Banner, Button, Dialog, Inputs, Menu, Typography } from '@/wax/components'
import type { ButtonSize } from '@/wax/components/button/button.css'

import { personName } from './person'
import type { ActionResult, FormContext, OrganizationPersonView } from './types'
import * as styles from './management.css'

/** The app shell's form context, for controls that sit outside any one page's loader. */
export const ShellFormContext = createContext<FormContext | undefined>(undefined)

export function NameInput({ label, defaultValue }: { label: string; defaultValue?: string }) {
  const id = useId()
  return (
    <div className={styles.field}>
      <Typography.BodySmallStrong as="label" htmlFor={id}>
        {label}
      </Typography.BodySmallStrong>
      <Inputs.TextInput
        id={id}
        name="displayName"
        defaultValue={defaultValue}
        required
        maxLength={256}
      />
    </div>
  )
}

export function Fields({
  form,
  intent,
  etag,
  name,
}: {
  form: FormContext
  intent: string
  etag?: string
  name?: string
}) {
  return (
    <>
      <input type="hidden" name="csrf" value={form.csrf} />
      <input type="hidden" name="requestId" value={form.requestId} />
      <input type="hidden" name="intent" value={intent} />
      {etag && <input type="hidden" name="etag" value={etag} />}
      {name && <input type="hidden" name="name" value={name} />}
    </>
  )
}

export function RoleField({
  label,
  defaultValue = 'member',
  disabled,
  formId,
  fullWidth,
  onChange,
}: {
  label: string
  defaultValue?: string
  disabled?: boolean
  fullWidth?: boolean
  formId?: string
  onChange?: (role: string) => void
}) {
  const [role, setRole] = useState(defaultValue)
  return (
    <>
      <input type="hidden" name="role" value={role} form={formId} />
      <Menu.Container>
        <Menu.Trigger
          render={
            <Button.Container
              ariaLabel={label}
              className={styles.roleTrigger}
              disabled={disabled}
              fullWidth={fullWidth}
              variant="secondary"
            />
          }
        >
          <Button.Text>{role === 'owner' ? 'Owner' : 'Member'}</Button.Text>
          <Button.Icon name="ChevronDown" />
        </Menu.Trigger>
        <Menu.Content align="start">
          <Menu.RadioGroup
            value={role}
            onValueChange={(value) => {
              setRole(value)
              onChange?.(value)
            }}
          >
            <Menu.RadioItem value="member">Member</Menu.RadioItem>
            <Menu.RadioItem value="owner">Owner</Menu.RadioItem>
          </Menu.RadioGroup>
        </Menu.Content>
      </Menu.Container>
    </>
  )
}

/**
 * Chooses one enabled organization member. The form field holds their
 * organization membership name, and stays empty until you choose.
 */
export function PersonField({
  label,
  name,
  people,
  currentUser,
  onChange,
}: {
  label: string
  /** The form field name. */
  name: string
  people: OrganizationPersonView[]
  currentUser: string
  onChange?: (membership: string) => void
}) {
  const [chosen, setChosen] = useState('')
  const text = (p: OrganizationPersonView) => personName(p.profile, currentUser)
  const person = people.find((p) => p.name === chosen)
  return (
    <div className={styles.field}>
      <Typography.BodySmallStrong as="span">{label}</Typography.BodySmallStrong>
      <input type="hidden" name={name} value={chosen} />
      <Menu.Container>
        <Menu.Trigger
          render={
            <Button.Container
              ariaLabel={person ? `${label}: ${text(person)}` : label}
              className={styles.roleTrigger}
              fullWidth
              variant="secondary"
            />
          }
        >
          <Button.Text>{person ? text(person) : 'Choose a person'}</Button.Text>
          <Button.Icon name="ChevronDown" />
        </Menu.Trigger>
        <Menu.Content align="start">
          <Menu.RadioGroup
            value={chosen}
            onValueChange={(value) => {
              setChosen(value)
              onChange?.(value)
            }}
          >
            {people.map((p) => (
              <Menu.RadioItem key={p.name} value={p.name}>
                {text(p)}
              </Menu.RadioItem>
            ))}
          </Menu.RadioGroup>
        </Menu.Content>
      </Menu.Container>
    </div>
  )
}

export function ConfirmAction({
  form,
  intent,
  etag,
  label,
  confirm,
  explanation,
  pending,
  name,
  children,
}: {
  name?: string
  children?: React.ReactNode
  form: FormContext
  intent: string
  etag: string
  label: string
  confirm: string
  explanation: string
  pending: boolean
}) {
  return (
    <ActionDialog
      title={label}
      description={explanation}
      submit={confirm}
      form={form}
      intent={intent}
      etag={etag}
      name={name}
      disabled={pending}
      destructive
    >
      {children}
    </ActionDialog>
  )
}

interface ActionDialogProps {
  title: string
  description?: string
  submit?: string
  form: FormContext
  intent: string
  action?: string
  etag?: string
  name?: string
  children?: React.ReactNode
  disabled?: boolean
  destructive?: boolean
  primary?: boolean
  open?: boolean
  onOpenChange?: (open: boolean) => void
  onSuccess?: (result: ActionResult) => void
  onError?: (result: ActionResult) => void
  size?: Dialog.PopupSize
  /** The size of the button that opens the dialog. */
  triggerSize?: ButtonSize
  finalFocus?: React.RefObject<HTMLElement | null>
}

export function ActionDialog(props: ActionDialogProps) {
  const [localOpen, setLocalOpen] = useState(false)
  const [pending, setPending] = useState(false)
  const open = props.open ?? localOpen
  const setOpen = props.onOpenChange ?? setLocalOpen
  return (
    <Dialog.Root
      open={open}
      onOpenChange={(next) => {
        if (!pending) setOpen(next)
      }}
    >
      {props.open === undefined && (
        <Dialog.Trigger
          render={
            <Button.Container
              disabled={props.disabled}
              size={props.triggerSize}
              variant={props.destructive ? 'destructive' : props.primary ? 'primary' : 'secondary'}
            />
          }
        >
          <Button.Text>{props.title}</Button.Text>
        </Dialog.Trigger>
      )}
      {open && (
        <Dialog.Portal>
          <Dialog.Backdrop />
          <ActionDialogBody {...props} onPendingChange={setPending} close={() => setOpen(false)} />
        </Dialog.Portal>
      )}
    </Dialog.Root>
  )
}

function ActionDialogBody({
  close,
  onPendingChange,
  onSuccess,
  onError,
  ...props
}: ActionDialogProps & { close: () => void; onPendingChange: (pending: boolean) => void }) {
  const fetcher = useFetcher<ActionResult>()
  const [submission] = useState({ form: props.form, etag: props.etag, name: props.name })
  const handled = useRef<ActionResult | undefined>(undefined)
  const pending = fetcher.state !== 'idle'
  useEffect(() => {
    onPendingChange(pending)
  }, [pending, onPendingChange])
  useEffect(() => {
    if (!fetcher.data || handled.current === fetcher.data) return
    // An error shows while the page reloads, so act on it then: waiting would
    // undo what the person changes in the meantime. Success closes the dialog
    // only once the page behind it is current.
    if (fetcher.data.error ? fetcher.state === 'submitting' : pending) return
    handled.current = fetcher.data
    if (fetcher.data.error) {
      onError?.(fetcher.data)
      return
    }
    onSuccess?.(fetcher.data)
    close()
  }, [pending, fetcher.state, fetcher.data, onSuccess, onError, close])
  return (
    <Dialog.Popup finalFocus={props.finalFocus} size={props.size}>
      <Dialog.Title>{props.title}</Dialog.Title>
      {props.description && <Dialog.Description>{props.description}</Dialog.Description>}
      <fetcher.Form
        method="post"
        action={props.action}
        className={styles.stack}
        aria-busy={pending}
        onSubmit={(event) => {
          if (pending || props.disabled) event.preventDefault()
        }}
      >
        <Fields
          form={submission.form}
          intent={props.intent}
          name={submission.name}
          etag={submission.etag}
        />
        {props.children}
        {fetcher.data?.error && <Banner variant="error">{fetcher.data.error}</Banner>}
        <Dialog.Actions>
          <Button.TextButton type="button" variant="secondary" disabled={pending} onClick={close}>
            Cancel
          </Button.TextButton>
          <Button.TextButton
            type="submit"
            variant={props.destructive ? 'destructive' : 'primary'}
            disabled={pending || props.disabled}
            busy={pending}
          >
            {props.submit ?? props.title}
          </Button.TextButton>
        </Dialog.Actions>
      </fetcher.Form>
    </Dialog.Popup>
  )
}

/** An error stays on the page; a success shows in the page itself. */
export function ActionNotice({ result }: { result?: ActionResult }) {
  return result?.error ? <Banner variant="error">{result.error}</Banner> : null
}
