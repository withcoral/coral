import {
  acceptOperations,
  attemptPath,
  type CreationAttempt,
} from '@/lib/provider-publication-attempt/model'
import { Button } from '@/wax/components'
import { OperationsSection, OperationsSummary } from './operations-section'
import { DiscoveryPrompt } from './setup-steps'
import type { FormContext, SetupView, StepperResult } from './types'
import { StepFrame } from './step-frame'

/**
 * Step two: what this provider can actually do. A protected MCP server lists
 * nothing until someone signs in, so the panel asks for that sign-in in place
 * of the list. Accepting the list moves the attempt on to the account question.
 */
export function OperationsStep({
  attempt,
  setup,
  basePath,
  backPath,
  discovering,
  form,
  pending,
  result,
  callbackUrl,
}: {
  attempt: CreationAttempt
  setup: SetupView
  basePath: string
  /** Where to revise the confirmed configuration. */
  backPath: string
  /** Coral cannot read the operations until this setup is authorized. */
  discovering: boolean
  form: FormContext
  pending: boolean
  result?: StepperResult
  callbackUrl: string
}) {
  return (
    <StepFrame
      title="Operations"
      meta={<OperationsSummary setup={setup} discovering={discovering} />}
      actions={
        <>
          <Button.InternalLink size="small" to={backPath} variant="secondary">
            Back
          </Button.InternalLink>
          {/* The step keeps its shape while it waits: there is nothing to accept
              until the list exists, so Next is here but cannot be taken. */}
          <Button.InternalLink
            size="small"
            to={attemptPath(basePath, acceptOperations(attempt))}
            variant="primary"
            disabled={discovering}
          >
            Next
          </Button.InternalLink>
        </>
      }
    >
      <OperationsSection
        setup={setup}
        discovery={
          discovering ? (
            <DiscoveryPrompt
              setup={setup}
              form={form}
              pending={pending}
              result={result}
              callbackUrl={callbackUrl}
            />
          ) : undefined
        }
      />
    </StepFrame>
  )
}
