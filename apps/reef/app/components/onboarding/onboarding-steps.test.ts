import { describe, expect, it } from 'vitest'

import { getOnboardingStepState, ONBOARDING_STEPS } from './onboarding-steps'

describe('onboarding steps', () => {
  it('derives position and navigation from the ordered step registry', () => {
    expect(ONBOARDING_STEPS).toEqual(['sources', 'query'])
    expect(getOnboardingStepState('sources')).toEqual({
      current: 1,
      nextHref: '?step=query',
      nextStep: 'query',
      step: 'sources',
      total: 2,
    })
    expect(getOnboardingStepState('query')).toEqual({
      current: 2,
      nextHref: null,
      nextStep: null,
      step: 'query',
      total: 2,
    })
  })

  it.each([null, 'unknown'])('defaults %s to the first step', (requestedStep) => {
    expect(getOnboardingStepState(requestedStep)).toEqual(getOnboardingStepState('sources'))
  })
})
