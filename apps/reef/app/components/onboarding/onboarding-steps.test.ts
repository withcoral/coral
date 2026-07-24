import { describe, expect, it } from 'vitest'

import { getOnboardingStepState, ONBOARDING_STEPS } from './onboarding-steps'

describe('onboarding steps', () => {
  it('derives position and navigation from the ordered step registry', () => {
    expect(ONBOARDING_STEPS).toEqual(['sources', 'query', 'next-steps'])
    expect(getOnboardingStepState('sources')).toEqual({
      current: 1,
      nextHref: '?step=query',
      nextStep: 'query',
      step: 'sources',
      total: 3,
    })
    expect(getOnboardingStepState('query')).toEqual({
      current: 2,
      nextHref: '?step=next-steps',
      nextStep: 'next-steps',
      step: 'query',
      total: 3,
    })
    expect(getOnboardingStepState('next-steps')).toEqual({
      current: 3,
      nextHref: null,
      nextStep: null,
      step: 'next-steps',
      total: 3,
    })
  })

  it.each([null, 'unknown'])('defaults %s to the first step', (requestedStep) => {
    expect(getOnboardingStepState(requestedStep)).toEqual(getOnboardingStepState('sources'))
  })
})
