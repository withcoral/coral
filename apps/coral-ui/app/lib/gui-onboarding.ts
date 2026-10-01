export const COMPLETE_ONBOARDING_INTENT = 'complete-onboarding'

export type CompleteGuiOnboardingError = {
  intent: typeof COMPLETE_ONBOARDING_INTENT
  message: string
  status: 'error'
}
