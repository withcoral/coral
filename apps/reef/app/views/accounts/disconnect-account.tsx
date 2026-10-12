import type { ReactNode } from 'react'
import { ActionDialog } from '@/views/management/forms'
import type { FormContext } from './types'

/** Confirms, then disconnects your own account for one provider. */
export function DisconnectAccount({
  form,
  action,
  children,
}: {
  form: FormContext
  action: string
  /** Hidden fields that identify the account where the action needs them. */
  children?: ReactNode
}) {
  return (
    <ActionDialog
      title="Disconnect"
      form={form}
      intent="disconnect"
      action={action}
      destructive
      description="Disconnect removes your stored credentials immediately. Coral stops unsent calls that use this account."
    >
      {children}
    </ActionDialog>
  )
}
