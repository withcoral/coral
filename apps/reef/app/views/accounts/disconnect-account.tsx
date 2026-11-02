import type { ReactNode } from 'react'
import type { ButtonSize } from '@/wax/components/button/button.css'
import { ActionDialog } from '@/views/management/forms'
import type { FormContext } from './types'

/** Confirms, then disconnects your own account for one provider. */
export function DisconnectAccount({
  form,
  action,
  children,
  size,
  quiet = false,
}: {
  form: FormContext
  action: string
  /** Hidden fields that identify the account where the action needs them. */
  children?: ReactNode
  size?: ButtonSize
  /** A secondary button, for a list where every row offers it. The confirmation stays red. */
  quiet?: boolean
}) {
  return (
    <ActionDialog
      title="Disconnect"
      form={form}
      intent="disconnect"
      action={action}
      destructive
      triggerSize={size}
      triggerVariant={quiet ? 'secondary' : undefined}
      description="Disconnect removes your stored credentials immediately. Coral stops unsent calls that use this account."
    >
      {children}
    </ActionDialog>
  )
}
