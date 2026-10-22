import { Form } from 'react-router'
import type { ButtonSize } from '@/wax/components/button/button.css'
import { DocumentSubmitButton } from '@/components/document-submit-button'

/** One click to your own account: POSTs to the provider's connect route. */
export function ConnectButton({
  action,
  csrf,
  requestId,
  label,
  ariaLabel,
  primary = true,
  disabled,
  size,
}: {
  action: string
  csrf: string
  /** Replays one authorization if the same click is submitted twice. */
  requestId?: string
  label: string
  ariaLabel?: string
  primary?: boolean
  disabled?: boolean
  size?: ButtonSize
}) {
  // The provider's sign-in is another site, so the browser loads the response itself.
  return (
    <Form method="post" action={action} reloadDocument>
      <input type="hidden" name="csrf" value={csrf} />
      <input type="hidden" name="intent" value="connect" />
      {requestId && <input type="hidden" name="requestId" value={requestId} />}
      <DocumentSubmitButton
        type="submit"
        variant={primary ? 'primary' : 'secondary'}
        disabled={disabled}
        size={size}
        aria-label={ariaLabel}
      >
        {label}
      </DocumentSubmitButton>
    </Form>
  )
}
