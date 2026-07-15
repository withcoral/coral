export const CORAL_UNAVAILABLE_STATUS = 503

export type CompleteGuiOnboardingError = {
  intent: 'complete-onboarding'
  message: string
  status: 'error'
}
