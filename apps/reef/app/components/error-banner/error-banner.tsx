import { Container as ButtonContainer } from '@/wax/components/button/container'
import { Icon as ButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Banner } from '@/wax/components/banner'

interface ErrorBannerProps {
  title?: string
  message: string
  onRetry?: () => void
}

export function ErrorBanner({ title, message, onRetry }: ErrorBannerProps) {
  return (
    <Banner
      action={
        onRetry ? (
          <ButtonContainer onClick={onRetry} size="22" variant="secondary">
            <ButtonIcon name="RefreshCw" />
            <ButtonText>Retry</ButtonText>
          </ButtonContainer>
        ) : undefined
      }
      title={title}
      variant="error"
    >
      {message}
    </Banner>
  )
}
