import { create } from '@bufbuild/protobuf'

import {
  CompleteGuiOnboardingRequestSchema,
  GetGuiOnboardingStateRequestSchema,
} from '@/generated/coral/v1/gui_onboarding_pb'

import { guiOnboardingClientForRequest } from './coral-request.server'

export async function getGuiOnboardingCompleted(
  request: Request,
  accessToken: string | null,
): Promise<boolean> {
  const client = guiOnboardingClientForRequest(request, accessToken)
  const response = await client.getGuiOnboardingState(create(GetGuiOnboardingStateRequestSchema), {
    signal: request.signal,
  })
  return response.completed
}

export async function completeGuiOnboarding(
  request: Request,
  accessToken: string | null,
): Promise<void> {
  const client = guiOnboardingClientForRequest(request, accessToken)
  await client.completeGuiOnboarding(create(CompleteGuiOnboardingRequestSchema))
}
