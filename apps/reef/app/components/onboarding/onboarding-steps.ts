export const ONBOARDING_STEPS = ['sources', 'query'] as const

export type OnboardingStep = (typeof ONBOARDING_STEPS)[number]

export interface OnboardingStepState {
  current: number
  nextHref: string | null
  nextStep: OnboardingStep | null
  step: OnboardingStep
  total: number
}

export function getOnboardingStepState(step: OnboardingStep): OnboardingStepState {
  const currentIndex = ONBOARDING_STEPS.indexOf(step)
  const nextStep = ONBOARDING_STEPS[currentIndex + 1] ?? null

  return {
    current: currentIndex + 1,
    nextHref: nextStep ? `?step=${nextStep}` : null,
    nextStep,
    step,
    total: ONBOARDING_STEPS.length,
  }
}
