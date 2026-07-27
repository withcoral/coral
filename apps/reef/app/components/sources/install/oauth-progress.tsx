import classNames from 'classnames'

import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

import type { OAuthInstallProgress } from '@/lib/source-oauth-install-flow'

import * as styles from './oauth-progress.css'
import * as statusStyles from './oauth-status.css'

export function OAuthProgress({
  authorizationUrl,
  className,
  inputLabel,
  userCode,
  verificationUri,
  verificationUriComplete,
}: {
  authorizationUrl: string
  className?: string
  inputLabel: string
  userCode: string
  verificationUri: string
  verificationUriComplete: string
}) {
  const link = verificationUriComplete || authorizationUrl
  const displayUri = verificationUri || authorizationUrl

  return (
    <div className={classNames(statusStyles.box, className)}>
      <Icon name="Loader" size="16" color="secondary" />
      <div>
        <Typography.BodySmall variant="primary">
          Waiting for {inputLabel} authorization in your browser…
        </Typography.BodySmall>
        {userCode ? (
          <>
            <Typography.BodySmall variant="secondary">
              Enter code <code className={styles.code}>{userCode}</code> at{' '}
              <a href={link} target="_blank" rel="noopener noreferrer">
                {displayUri}
              </a>
              .
            </Typography.BodySmall>
            <Typography.BodySmall variant="tertiary">
              If the new tab didn't open, use the link above.
            </Typography.BodySmall>
          </>
        ) : (
          <Typography.BodySmall variant="tertiary">
            If the new tab didn't open,{' '}
            <a href={authorizationUrl} target="_blank" rel="noopener noreferrer">
              click here to open it
            </a>
            .
          </Typography.BodySmall>
        )}
      </div>
    </div>
  )
}

export function OAuthInstallStatus({
  className,
  inputLabel,
  progress,
}: {
  className?: string
  inputLabel: (inputKey: string) => string
  progress: OAuthInstallProgress
}) {
  if (progress.kind === 'awaiting-oauth') {
    return (
      <OAuthProgress
        authorizationUrl={progress.authorizationUrl}
        className={className}
        inputLabel={inputLabel(progress.inputKey)}
        userCode={progress.userCode}
        verificationUri={progress.verificationUri}
        verificationUriComplete={progress.verificationUriComplete}
      />
    )
  }
  if (progress.kind === 'oauth-completed') {
    return (
      <OAuthStatus className={className} icon="CircleCheck" iconColor="success">
        {inputLabel(progress.inputKey)} authorized. Finishing install…
      </OAuthStatus>
    )
  }
  if (progress.kind === 'oauth-callback-received') {
    return (
      <OAuthStatus className={className} icon="Loader" iconColor="secondary">
        {inputLabel(progress.inputKey)} authorization received. Exchanging token…
      </OAuthStatus>
    )
  }
  if (progress.kind === 'success') {
    return (
      <OAuthStatus className={className} icon="CircleCheck" iconColor="success">
        {inputLabel(progress.name)} configured.
      </OAuthStatus>
    )
  }
  return null
}

function OAuthStatus({
  className,
  children,
  icon,
  iconColor,
}: {
  className?: string
  children: React.ReactNode
  icon: 'CircleCheck' | 'Loader'
  iconColor: 'secondary' | 'success'
}) {
  return (
    <div className={classNames(statusStyles.box, className)}>
      <Icon name={icon} size="16" color={iconColor} />
      <Typography.BodySmall variant="primary">{children}</Typography.BodySmall>
    </div>
  )
}
