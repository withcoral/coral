import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

import * as styles from './oauth-progress.css'
import * as statusStyles from './oauth-status.css'

export function OAuthProgress({
  authorizationUrl,
  inputLabel,
  userCode,
  verificationUri,
  verificationUriComplete,
}: {
  authorizationUrl: string
  inputLabel: string
  userCode: string
  verificationUri: string
  verificationUriComplete: string
}) {
  const link = verificationUriComplete || authorizationUrl
  const displayUri = verificationUri || authorizationUrl

  return (
    <div className={statusStyles.box}>
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
