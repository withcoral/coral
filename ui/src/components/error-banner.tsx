import { Container as ButtonContainer } from '@/wax/components/button/container'
import { Icon as ButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

import * as styles from './error-banner.css'

interface ErrorBannerProps {
  title?: string
  message: string
  onRetry?: () => void
}

export function ErrorBanner({ title, message, onRetry }: ErrorBannerProps) {
  return (
    <div className={styles.banner} role="alert">
      <Icon name="CircleAlert" size="18" color="error" />
      <div className={styles.text}>
        {title ? <Typography.BodySmallStrong>{title}</Typography.BodySmallStrong> : null}
        <Typography.BodySmall variant="secondary">{message}</Typography.BodySmall>
      </div>
      {onRetry ? (
        <ButtonContainer variant="secondary" size="22" onClick={onRetry}>
          <ButtonIcon name="RefreshCw" />
          <ButtonText>Retry</ButtonText>
        </ButtonContainer>
      ) : null}
    </div>
  )
}
