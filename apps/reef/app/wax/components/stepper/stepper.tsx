import type { ReactNode } from 'react'

import { Button } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

import * as styles from './stepper.css'

export type StepState = 'done' | 'current' | 'upcoming'

export interface Step {
  /** The step's name, such as "Source". */
  title: string
  state: StepState
  /** What a done step decided, in a few words. */
  summary?: ReactNode
  /** Where a done step can be revisited. */
  change?: { to: string; label?: string }
  /** The current step's controls. Ignored for other states. */
  content?: ReactNode
}

export interface StepperProps {
  /** The accessible name of the whole sequence. */
  label: string
  steps: readonly Step[]
  /** Rail keeps every step above one current screen. Inline retains completed summaries. */
  layout?: 'inline' | 'rail'
  /** Hide the rail until the journey has started; the first screen needs no map. */
  showRail?: boolean
}

/**
 * Numbered steps in one journey. Exactly one step is current and carries its
 * controls; done steps keep a one-line summary and a way back.
 */
export function Stepper({ label, steps, layout = 'inline', showRail = true }: StepperProps) {
  const current = steps.find((step) => step.state === 'current')
  if (layout === 'rail')
    return (
      <div className={styles.journey}>
        {showRail && (
          <ol className={styles.rail} aria-label={label}>
            {steps.map((step, index) => (
              <li
                key={step.title}
                className={styles.railStep}
                aria-current={step.state === 'current' ? 'step' : undefined}
              >
                <span className={styles.marker({ state: step.state })} aria-hidden>
                  {step.state === 'done' ? (
                    <Icon name="Check" size="16" color="inherit" />
                  ) : (
                    index + 1
                  )}
                </span>
                <span className={styles.railText}>
                  {step.state === 'current' && (
                    <Typography.BodySmall
                      as="span"
                      variant="tertiary"
                      className={styles.railCounter}
                    >
                      Step {index + 1} of {steps.length}
                    </Typography.BodySmall>
                  )}
                  {step.state === 'done' && step.change ? (
                    <Button.InternalLink
                      to={step.change.to}
                      variant="link"
                      size="x-small"
                      aria-label={`${step.change.label ?? 'Change'}: ${step.title}`}
                    >
                      {step.title}
                    </Button.InternalLink>
                  ) : (
                    <Typography.BodySmallStrong
                      as="span"
                      variant={step.state === 'upcoming' ? 'tertiary' : 'primary'}
                    >
                      {step.title}
                    </Typography.BodySmallStrong>
                  )}
                  {step.state === 'done' && step.summary && (
                    <Typography.BodySmall as="span" variant="tertiary" truncate>
                      {step.summary}
                    </Typography.BodySmall>
                  )}
                </span>
              </li>
            ))}
          </ol>
        )}
        {current && (
          <section className={styles.screen} aria-label={current.title}>
            <Typography.HeadingSmall as="h2">{current.title}</Typography.HeadingSmall>
            {current.content}
          </section>
        )}
      </div>
    )
  return (
    <ol className={styles.list} aria-label={label}>
      {steps.map((step, index) => (
        <li
          key={step.title}
          className={styles.step({ state: step.state })}
          aria-current={step.state === 'current' ? 'step' : undefined}
        >
          <span className={styles.marker({ state: step.state })} aria-hidden>
            {step.state === 'done' ? <Icon name="Check" size="16" color="inherit" /> : index + 1}
          </span>
          <div className={styles.heading}>
            {step.state === 'current' ? (
              <Typography.HeadingSmall as="h2">{step.title}</Typography.HeadingSmall>
            ) : (
              <Typography.BodyStrong
                as="h2"
                variant={step.state === 'upcoming' ? 'tertiary' : 'primary'}
              >
                {step.title}
              </Typography.BodyStrong>
            )}
            {step.state === 'done' && step.summary && (
              <Typography.BodySmall as="span" className={styles.summary}>
                {step.summary}
              </Typography.BodySmall>
            )}
            {step.state === 'done' && step.change && (
              <Button.InternalLink to={step.change.to} variant="link" size="x-small">
                {step.change.label ?? 'Change'}
              </Button.InternalLink>
            )}
          </div>
          {step.state === 'current' && step.content && (
            <div className={styles.content}>{step.content}</div>
          )}
        </li>
      ))}
    </ol>
  )
}
