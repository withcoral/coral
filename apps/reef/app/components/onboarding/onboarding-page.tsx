import classNames from 'classnames'
import type { AnchorHTMLAttributes, ReactNode } from 'react'
import { Link, type To } from 'react-router'

import { Button, Typography } from '@/wax/components'
import { Pill } from '@/wax/components/pill'

import * as styles from './onboarding-page.css'

interface OnboardingPageActionBase {
  disabled?: boolean
  label: string
}

interface OnboardingPageButtonAction extends OnboardingPageActionBase {
  onClick?: () => void
  to?: never
}

interface OnboardingPageLinkAction extends OnboardingPageActionBase {
  onClick?: never
  to: To
}

export type OnboardingPageAction = OnboardingPageButtonAction | OnboardingPageLinkAction

export interface OnboardingPageProps {
  action?: OnboardingPageAction
  ariaLabel?: string
  children: ReactNode
  mainFrameClassName?: string
  sideContent: ReactNode
  sideTitle: string
  stepLabel?: string
  title?: string
}

export function OnboardingPage({
  action,
  ariaLabel = 'Onboarding',
  children,
  mainFrameClassName,
  sideContent,
  sideTitle,
  stepLabel,
  title = 'Onboarding',
}: OnboardingPageProps) {
  return (
    <section className={styles.root} aria-label={ariaLabel}>
      <div className={styles.content}>
        <header className={styles.header}>
          <div className={styles.headerText}>
            <div className={styles.titleRow}>
              <Typography.HeadingLarge as="h1">{title}</Typography.HeadingLarge>
              {stepLabel ? (
                <Pill className={styles.stepPill} color="graySubtle">
                  {stepLabel}
                </Pill>
              ) : null}
            </div>
          </div>
        </header>

        <div className={styles.body}>
          <div className={styles.explainer}>
            <div className={styles.explainerText}>
              <Typography.HeadingSmall>{sideTitle}</Typography.HeadingSmall>
              {sideContent}
            </div>

            {action ? <OnboardingAction action={action} /> : null}
          </div>

          <div className={classNames(styles.mainFrame, mainFrameClassName)}>{children}</div>
        </div>
      </div>
    </section>
  )
}

function OnboardingAction({ action }: { action: OnboardingPageAction }) {
  if (action.to !== undefined) {
    return (
      <Button.Container
        as={Link}
        disabled={action.disabled}
        onClick={action.disabled ? (event) => event.preventDefault() : undefined}
        to={action.to}
        variant="secondary"
      >
        <Button.Text>{action.label}</Button.Text>
      </Button.Container>
    )
  }

  return (
    <Button.Container disabled={action.disabled} onClick={action.onClick} variant="secondary">
      <Button.Text>{action.label}</Button.Text>
    </Button.Container>
  )
}

export function OnboardingLink({
  className,
  rel = 'noopener noreferrer',
  target = '_blank',
  ...props
}: AnchorHTMLAttributes<HTMLAnchorElement>) {
  return (
    <a className={classNames(styles.inlineLink, className)} rel={rel} target={target} {...props} />
  )
}
