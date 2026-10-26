import { Button, Card, Typography } from '@/wax/components'
import * as styles from './management.css'

/**
 * The prompt's first three lines on a card. With `copyLabel`, the card holds an icon
 * button with that accessible name, which copies the whole prompt.
 */
export function PromptPreview({ prompt, copyLabel }: { prompt: string; copyLabel?: string }) {
  return (
    <Card.Container compact className={copyLabel && styles.promptCopy}>
      <Typography.BodySmall as="p" className={styles.onboardingPrompt}>
        {prompt}
      </Typography.BodySmall>
      {copyLabel && (
        <Button.CopyButton
          variant="bare"
          ariaLabel={copyLabel}
          tooltipText={copyLabel}
          textToCopy={prompt}
        />
      )}
    </Card.Container>
  )
}
