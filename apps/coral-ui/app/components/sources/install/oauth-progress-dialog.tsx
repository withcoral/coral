import classNames from 'classnames'

import { Button, Dialog } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

import type { OAuthInstallProgress } from '@/lib/source-oauth-install-flow'

import * as styles from './oauth-progress-dialog.css'

export function OAuthProgressDialog({
  error,
  inputLabel,
  onCancel,
  progress,
}: {
  error: string | null
  inputLabel: (inputKey: string) => string
  onCancel: () => void
  progress: OAuthInstallProgress
}) {
  const hasError = error !== null
  const content = dialogContent(error, inputLabel, progress)
  const awaitingOAuth = progress.kind === 'awaiting-oauth' ? progress : null

  return (
    <Dialog.Root
      open={progress.kind !== 'idle' || hasError}
      onOpenChange={(open) => {
        if (!open) onCancel()
      }}
    >
      <Dialog.Portal>
        <Dialog.Popup size="m">
          <Dialog.Title>{content.title}</Dialog.Title>
          <Dialog.Description
            className={classNames({
              [styles.error]: hasError,
              [styles.status]: content.icon,
            })}
            role={hasError ? 'alert' : content.icon ? 'status' : undefined}
          >
            {content.icon ? (
              <Icon color={content.iconColor ?? 'secondary'} name={content.icon} size="18" />
            ) : null}
            {content.description}
          </Dialog.Description>
          {awaitingOAuth?.userCode ? (
            <div className={styles.codePanel}>
              <Typography.BodySmall variant="tertiary">Enter this code</Typography.BodySmall>
              <div className={styles.codeRow}>
                <Typography.CodeLarge
                  as="code"
                  className={styles.code}
                  size={24}
                  variant="primary"
                  weight={700}
                >
                  {awaitingOAuth.userCode}
                </Typography.CodeLarge>
                <Button.CopyButton
                  ariaLabel="Copy device code to clipboard"
                  textToCopy={awaitingOAuth.userCode}
                ></Button.CopyButton>
              </div>
            </div>
          ) : null}
          <Dialog.Actions>
            <Button.TextButton onClick={onCancel} variant="secondary">
              {hasError ? 'Back' : progress.kind === 'success' ? 'Close' : 'Cancel'}
            </Button.TextButton>
            {awaitingOAuth ? (
              <Button.Container
                as="a"
                href={awaitingOAuth.verificationUriComplete || awaitingOAuth.authorizationUrl}
                rel="noopener noreferrer"
                target="_blank"
                variant="primary"
              >
                <Button.Text>Open authorization page</Button.Text>
                <Button.Icon name="ExternalLink" />
              </Button.Container>
            ) : progress.kind !== 'success' && !hasError ? (
              <Button.Container disabled variant="primary">
                <Button.Text>Open authorization page</Button.Text>
                <Button.SpinningButtonIcon name="Loader" />
              </Button.Container>
            ) : null}
          </Dialog.Actions>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function dialogContent(
  error: string | null,
  inputLabel: (inputKey: string) => string,
  progress: OAuthInstallProgress,
): {
  description: string
  icon?: 'CircleAlert' | 'CircleCheck'
  iconColor?: 'error' | 'secondary' | 'success'
  title: string
} {
  if (error !== null) {
    return {
      description: error,
      icon: 'CircleAlert',
      iconColor: 'error',
      title: 'Couldn’t complete authorization',
    }
  }

  switch (progress.kind) {
    case 'awaiting-oauth':
      return {
        description: 'Complete authorization in your browser.',
        title: `Authorize ${inputLabel(progress.inputKey)}`,
      }
    case 'oauth-callback-received':
      return {
        description: `Exchanging the ${inputLabel(progress.inputKey)} authorization for a token…`,
        title: 'Authorization received',
      }
    case 'oauth-completed':
      return {
        description: `${inputLabel(progress.inputKey)} authorized. Configuring source…`,
        title: 'Finishing setup',
      }
    case 'success':
      return {
        description: 'Finishing up…',
        icon: 'CircleCheck',
        iconColor: 'success',
        title: 'Source configured',
      }
    default:
      return {
        description: 'Starting authorization…',
        title: 'Connecting with OAuth',
      }
  }
}
