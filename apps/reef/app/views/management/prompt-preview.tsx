import { Button, Card, Typography } from '@/wax/components'
import { promptCopyFailure } from './onboarding-prompt'
import * as styles from './management.css'

/**
 * The prompt's first three lines on a card, with an icon button that has the accessible
 * name `copyLabel` and copies the whole prompt.
 */
export function PromptPreview({ prompt, copyLabel }: { prompt: string; copyLabel: string }) {
  return (
    <Card.Container compact className={styles.promptCopy}>
      <Typography.BodySmall as="p" className={styles.onboardingPrompt}>
        {prompt}
      </Typography.BodySmall>
      <Button.CopyButton
        variant="bare"
        ariaLabel={copyLabel}
        failureDescription={promptCopyFailure}
        tooltipText={copyLabel}
        textToCopy={prompt}
      />
    </Card.Container>
  )
}
