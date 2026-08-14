import { create } from '@bufbuild/protobuf'

import { CompleteGuiOnboardingRequestSchema } from '@/generated/coral/v1/gui_onboarding_pb'

import { guiOnboardingClientForRequest } from './coral-request.server'

export async function completeGuiOnboarding(
  request: Request,
  accessToken: string | null,
): Promise<void> {
  const client = guiOnboardingClientForRequest(request, accessToken)
  await client.completeGuiOnboarding(create(CompleteGuiOnboardingRequestSchema))
}
