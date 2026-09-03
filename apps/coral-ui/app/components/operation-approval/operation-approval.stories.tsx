import type { CSSProperties } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'

import { type } from 'arktype'
import { useId, useState } from 'react'
import { fn } from 'storybook/test'

import { Button, Dialog, Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'
import { theme } from '@/wax/theme/theme.css'

type ArgumentValue =
  | string
  | number
  | boolean
  | null
  | ArgumentValue[]
  | { [key: string]: ArgumentValue }
type DatasetKey = 'github' | 'linear' | 'loop' | 'savedFunction'
type EnvelopeMatch = 'inside' | 'outside'
type EnvelopeArgsDisplay = 'interleaved' | 'none' | 'summary'

interface OperationApprovalStoryProps {
  argumentsCollapsible?: boolean
  argumentsInitiallyExpanded?: boolean
  envelopeArgsDisplay?: EnvelopeArgsDisplay
  envelopeMatch: EnvelopeMatch
  compact?: boolean
  dataset: DatasetKey
  onApprove: () => void
  onDecline: () => void
  onUpdatePolicy: () => void
  onViewRun: () => void
  showArguments?: boolean
  showApprovalAuthority?: boolean
  showExpiry?: boolean
  showAuthorityEnvelopeMatch?: boolean
  showIdentity?: boolean
  showProgramBody?: boolean
  showProgramSnippet?: boolean
  showProviderReference?: boolean
  showRequestMetadataInHeader?: boolean
  showRequestContext?: boolean
  showRequester?: boolean
  showRunContext?: boolean
  showTechnicalDetails?: boolean
}

type OperationApprovalProps = Omit<OperationApprovalStoryProps, 'dataset' | 'envelopeMatch'> & {
  approval: OperationApprovalModel
  authorityEnvelope: AuthorityEnvelope
}

interface ProgramEvidence {
  after?: string
  before?: string
  currentOperation: string
}

interface ProgramContext {
  body: ProgramEvidence
  snippet: ProgramEvidence
}

interface RequestContext {
  execIntent: string
  taskId: string
  taskIntent: string
}

type EnvelopeCheckStatus = 'fail' | 'pass' | 'unknown'
type PolicyProposalKind = 'automatic' | 'manual' | 'unavailable'

interface PolicyProposal {
  kind: PolicyProposalKind
  proposedPolicyChange: string
  reason?: string
}

const envelopeStatusColors: Record<EnvelopeCheckStatus, 'amber' | 'green' | 'red'> = {
  fail: 'red',
  pass: 'green',
  unknown: 'amber',
}

const envelopeStatusLabels: Record<EnvelopeCheckStatus, string> = {
  fail: 'Fails',
  pass: 'Pass',
  unknown: 'Unknown',
}

interface AuthorityEnvelopeEvaluation {
  checks: Array<{
    label: string
    observed: string
    policy: string
    proposal?: PolicyProposal
    status: EnvelopeCheckStatus
  }>
  decision: 'allow' | 'requiresApproval'
  envelopeId: string
  expiresAt: string
  installedBy: string
}

interface AuthorityEnvelope {
  envelopeId: string
  evaluatedAt: string
  expiresAt: string
  installedBy: string
  operationCallPath: string
  rules: AuthorityEnvelopeRule[]
}

interface AuthorityEnvelopeRule {
  evaluate: (approval: OperationApprovalModel) => {
    observed: string
    proposal?: PolicyProposal
    status: EnvelopeCheckStatus
  }
  label: string
  policy: string
}

interface OperationApprovalDataset {
  approval: OperationApprovalModel
  authorityEnvelopes: Record<EnvelopeMatch, AuthorityEnvelope>
}

interface OperationApprovalModel {
  approvalAuthority: string
  expiresAt: string
  identity: string
  invocationArguments: Array<{ label: string; value: ArgumentValue }>
  invokingPrincipal: string
  operationCallPath: string
  policyText: string
  programContext?: ProgramContext
  provider: string
  providerReference?: string
  rawInvocation: ArgumentValue[]
  requestContext?: RequestContext
  runContext: {
    runId: string
    status: 'running'
    workspace: string
  }
  technicalDetails: Array<{ label: string; value: string }>
}

const meta = {
  argTypes: {
    envelopeMatch: {
      control: 'inline-radio',
      description: 'Choose the Owner envelope evaluated against the selected dataset.',
      name: 'Match envelope',
      options: ['inside', 'outside'],
    },
    dataset: {
      control: 'select',
      options: ['github', 'linear', 'loop', 'savedFunction'],
    },
    showAuthorityEnvelopeMatch: {
      control: 'boolean',
      description: 'Execute and show a Storybook-only deterministic Owner-policy envelope.',
    },
    envelopeArgsDisplay: {
      control: 'inline-radio',
      description: 'Show selected Owner-envelope checks beside exact invocation arguments.',
      options: ['none', 'summary', 'interleaved'],
    },
    showRequestContext: {
      control: 'boolean',
      description: 'Show Task and exec intent as secondary request context.',
    },
    showProviderReference: {
      control: 'boolean',
      description: 'Show optional provider-authored reference copy as a quiet disclosure.',
    },
  },
  args: {
    envelopeMatch: 'inside',
    dataset: 'github',
    onApprove: fn(),
    onDecline: fn(),
    onUpdatePolicy: fn(),
    onViewRun: fn(),
    showRequestContext: false,
    showProviderReference: false,
    showAuthorityEnvelopeMatch: false,
    envelopeArgsDisplay: 'none',
  },
  component: OperationApprovalStory,
  decorators: [
    (Story) => (
      <div style={storyCanvasStyle}>
        <Story />
      </div>
    ),
  ],
  parameters: {
    backgrounds: { default: 'dark' },
    docs: {
      description: {
        component: `An Operation Approval resolves one pending Operation Invocation. It does not approve the Program Run, future Invocations, or a reusable permission profile. The containing Program Run remains running while it waits for the decision.

The story-only model uses Lagoon-aligned names: \`operationCallPath\`, \`invocationArguments\`, \`invokingPrincipal\`, \`approvalAuthority\`, \`expiresAt\`, optional \`requestContext\`, and optional \`programContext\`. Task and exec intent are request context; they do not replace the exact Invocation arguments or describe the consequence of approval.

Every Medium+ story keeps the same first-screen decision contract: Operation call path, provider identity, invoking principal, approval authority, expiry, exact Invocation arguments, and the unchanged Decline/Approve actions. The prototype does not invent an operation-specific consequence CTA when no trusted renderer provides one.

Every story uses the same story-local \`OperationApproval\` component. Toggle \`showRequestContext\` in any story to reveal Task and exec intent below Arguments; \`TaskIntentContext\` is the preset that enables it by default.

Every dataset supplies two Storybook-only Owner envelopes: \`inside\` matches its exact Invocation and \`outside\` fails explicit argument filters. The \`dataset\` and \`envelopeMatch\` controls select those two dimensions independently. ArkType executes each envelope’s operation, argument-path, and expiry rules; it is not proposed as Coral’s production authorization engine. These stories do not model an agent approving another agent, and a passing envelope applies only to the exact Invocation shown. Toggle \`showAuthorityEnvelopeMatch\` to reveal the full evaluation. Use \`envelopeArgsDisplay\` to show no argument annotations, a positional-argument summary, or policy checks interleaved with their exact fields.

Each story has a **dataset** control. The default GitHub dataset uses \`coral.providers.github.issues.createComment\`; the Linear dataset uses \`coral.providers.linear.issues.update\` inside a program that also reads GitHub context; the loop dataset puts one pending \`coral.providers.linear.issues.update\` Invocation inside a \`for\` loop; the saved-function dataset shows one pending Operation from a linked \`coral.functions.postApprovalFollowUp\` source snapshot. Within a selected dataset, the stories keep the operation/request stable so reviewers can compare disclosure and rendering choices without changing provider, operation, requester, identity, or run context.

- **Minimal** uses the stable operation name with very little extra detail.
- **MinimalWithExpiry** adds only the approval deadline.
- **MinimalWithRequester** adds only the requesting principal.
- **MinimalWithRequesterAndExpiry** adds both compact approval-request fields.
- **Medium** adds identity plus positional/raw arguments.
- **MediumWithExpiry**, **MediumWithRequester**, and **MediumWithRequesterAndExpiry** retain earlier comparison names, but now preserve the complete Medium+ decision contract.
- **MediumExpandableExpanded** includes requester and expiry, starts open, and allows arguments to be collapsed.
- **MediumExpandableCollapsed** includes requester and expiry behind a collapsed-by-default argument review.
- **TaskIntentContext** adds grounded Task and MCP exec intent as secondary request context.
- **EnvelopeMatches** shows an exact Invocation that passes every known Owner-policy check.
- **EnvelopeDoesNotMatch** shows exact failed and unknown checks and retains per-call approval.
- **EnvelopePolicyUpdateProposal** keeps one-time approval separate from reviewing a bounded future Owner-policy change.
- **ProgramSnippet** adds expandable arguments and highlights one current-operation call as supporting context.
- **CollapsedProgramBody** adds expandable arguments and a collapsed, self-contained Program body.
- **MaximalEvidence** keeps arguments expandable while exposing snippet, Program body, provider reference copy, raw args, and ids.

Use the least disclosure that still makes the concrete target clear. Program body helps explain surrounding intent and flow, but it is supporting evidence, not the source of truth for concrete arguments. Provider-authored descriptions are uncontrolled reference material and never carry the core approval explanation.`,
      },
    },
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  title: 'Components/OperationApproval',
} satisfies Meta<typeof OperationApprovalStory>

export default meta
type Story = StoryObj<typeof meta>

function evaluateAuthorityEnvelope(
  approval: OperationApprovalModel,
  envelope: AuthorityEnvelope,
): AuthorityEnvelopeEvaluation {
  const operationPolicy = type('string').narrow((value) => value === envelope.operationCallPath)
  const expiryPolicy = type('Date').narrow(
    (evaluatedAt) => evaluatedAt.getTime() < new Date(envelope.expiresAt).getTime(),
  )
  const checks: AuthorityEnvelopeEvaluation['checks'] = [
    {
      label: 'Operation call path',
      observed: approval.operationCallPath,
      policy: `== ${JSON.stringify(envelope.operationCallPath)}`,
      status: arkStatus(operationPolicy(approval.operationCallPath)),
    },
    ...envelope.rules.map(({ evaluate, label, policy }) => ({
      label,
      policy,
      ...evaluate(approval),
    })),
    {
      label: 'Policy expiry',
      observed: `Evaluated ${envelope.evaluatedAt}`,
      policy: `expires ${envelope.expiresAt}`,
      status: arkStatus(expiryPolicy(new Date(envelope.evaluatedAt))),
    },
  ]

  return {
    checks,
    decision: checks.every(({ status }) => status === 'pass') ? 'allow' : 'requiresApproval',
    envelopeId: envelope.envelopeId,
    expiresAt: envelope.expiresAt,
    installedBy: envelope.installedBy,
  }
}

function arkStatus(result: unknown): EnvelopeCheckStatus {
  return result instanceof type.errors ? 'fail' : 'pass'
}

const approvalDatasets: Record<DatasetKey, OperationApprovalModel> = {
  github: {
    invocationArguments: [
      {
        label: 'Argument 1',
        value: {
          issue_number: 85,
          body: 'The approval queue now groups pending actions by Program Run and updates live.',
          org: 'withcoral',
          repo: 'lagoon',
        },
      },
      {
        label: 'Argument 2',
        value: {
          format: 'markdown',
          mentions: ['@reef-team'],
        },
      },
      { label: 'Argument 3', value: 'notify-requester' },
    ],
    approvalAuthority: 'Workspace owner',
    expiresAt: '14:42 UTC',
    identity: 'coral-bot',
    operationCallPath: 'coral.providers.github.issues.createComment',
    policyText: 'Agents cannot approve their own operations.',
    programContext: {
      body: {
        before: `const selectedIssue = { org: "withcoral", repo: "lagoon", number: 85 }
const comment = {
  issue_number: selectedIssue.number,
  org: selectedIssue.org,
  repo: selectedIssue.repo,
  body: "The approval queue now groups pending actions by Program Run and updates live.",
}
const options = { format: "markdown", mentions: ["@reef-team"] }
const routing = "notify-requester"`,
        currentOperation: 'await github.issues.createComment(comment, options, routing)',
        after: `await audit.log({ action: "requested_github_comment", issue: selectedIssue.number })`,
      },
      snippet: {
        before: `const body = "The approval queue now groups pending actions by Program Run and updates live."
const comment = { org: "withcoral", repo: "lagoon", issue_number: 85, body }
const options = { format: "markdown", mentions: ["@reef-team"] }`,
        currentOperation: 'await github.issues.createComment(comment, options, "notify-requester")',
      },
    },
    provider: 'GitHub',
    providerReference:
      'Creates a comment on an issue in the selected repository using the authenticated GitHub account.',
    rawInvocation: [
      {
        issue_number: 85,
        body: 'The approval queue now groups pending actions by Program Run and updates live.',
        org: 'withcoral',
        repo: 'lagoon',
      },
      {
        format: 'markdown',
        mentions: ['@reef-team'],
      },
      'notify-requester',
    ],
    invokingPrincipal: 'reef-agent',
    requestContext: {
      execIntent: 'Post the prepared approval-card update to the Lagoon issue.',
      taskId: 'task_01K3Q7ZX',
      taskIntent: 'Follow up on the approval-card review for the Run Status Page.',
    },
    runContext: {
      runId: 'run_01K3Q8DA',
      status: 'running',
      workspace: 'Lagoon',
    },
    technicalDetails: [
      { label: 'Approval request', value: 'apr_01K3Q8F1 · pending · created 14:34 UTC' },
      { label: 'Operation invocation', value: 'inv_01K3Q8E4 · pending' },
      { label: 'Provider generation', value: 'github@gen_0198' },
      { label: 'Event cursor', value: 'evt_00000047' },
      { label: 'Trace', value: '8f5c1f45b2ef4d65' },
    ],
  },
  linear: {
    invocationArguments: [
      {
        label: 'Argument 1',
        value: {
          issue_id: 'LIN-482',
          state_id: 'in_review',
          comment: 'Linked GitHub issue with the run-status approval follow-up.',
          labels: ['run-status', 'needs-review'],
          source: { provider: 'github', issue: 'withcoral/lagoon#85' },
        },
      },
      { label: 'Argument 2', value: { notifyAssignee: true, priority: 'normal' } },
      { label: 'Argument 3', value: 'approval-card-review' },
    ],
    approvalAuthority: 'Workspace owner',
    expiresAt: '14:42 UTC',
    identity: 'coral-linear-bot',
    operationCallPath: 'coral.providers.linear.issues.update',
    policyText: 'Agents cannot approve their own operations.',
    programContext: {
      body: {
        before: `const githubIssue = { repo: "withcoral/lagoon", number: 85 }
const linearIssue = { id: "LIN-482", state: "in_review" }
const updateInput = {
  issue_id: linearIssue.id,
  state_id: linearIssue.state,
  comment: "Linked GitHub issue with the run-status approval follow-up.",
  labels: ["run-status", "needs-review"],
  source: { provider: "github", issue: githubIssue.repo + "#" + githubIssue.number },
}
const options = { notifyAssignee: true, priority: "normal" }`,
        currentOperation:
          'await linear.issues.update(updateInput, options, "approval-card-review")',
        after: `await audit.log({ action: "requested_linear_update", issue: linearIssue.id })`,
      },
      snippet: {
        before: `const githubIssue = { repo: "withcoral/lagoon", number: 85 }
const updateInput = { issue_id: "LIN-482", state_id: "in_review", source: githubIssue }
const options = { notifyAssignee: true, priority: "normal" }`,
        currentOperation:
          'await linear.issues.update(updateInput, options, "approval-card-review")',
      },
    },
    provider: 'Linear',
    providerReference: 'Updates an issue using the authenticated Linear workspace identity.',
    rawInvocation: [
      {
        issue_id: 'LIN-482',
        state_id: 'in_review',
        comment: 'Linked GitHub issue with the run-status approval follow-up.',
        labels: ['run-status', 'needs-review'],
        source: { provider: 'github', issue: 'withcoral/lagoon#85' },
      },
      { notifyAssignee: true, priority: 'normal' },
      'approval-card-review',
    ],
    invokingPrincipal: 'reef-agent',
    requestContext: {
      execIntent: 'Update the Linear review item with the related GitHub issue context.',
      taskId: 'task_01K3Q9B2',
      taskIntent: 'Coordinate the approval-card review across Linear and GitHub.',
    },
    runContext: {
      runId: 'run_01K3Q8DA',
      status: 'running',
      workspace: 'Lagoon',
    },
    technicalDetails: [
      { label: 'Approval request', value: 'apr_01K3Q8F1 · pending · created 14:34 UTC' },
      { label: 'Operation invocation', value: 'inv_01K3Q8E4 · pending' },
      { label: 'Provider generation', value: 'linear@gen_0198' },
      { label: 'Event cursor', value: 'evt_00000047' },
      { label: 'Trace', value: '8f5c1f45b2ef4d65' },
    ],
  },
  loop: {
    invocationArguments: [
      {
        label: 'Argument 1',
        value: {
          issue_id: 'LIN-491',
          state_id: 'triaged',
          note: 'Iteration 2/5: linked GitHub issue with matching approval-card feedback.',
          source: { provider: 'github', issue: 'withcoral/lagoon#91' },
        },
      },
      { label: 'Argument 2', value: { notifyAssignee: false, loopIndex: 2, total: 5 } },
      { label: 'Argument 3', value: 'loop-iteration-2' },
    ],
    approvalAuthority: 'Workspace owner',
    expiresAt: '14:42 UTC',
    identity: 'coral-linear-bot',
    operationCallPath: 'coral.providers.linear.issues.update',
    policyText: 'Agents cannot approve their own operations.',
    programContext: {
      body: {
        before: `const githubIssues = [91, 92, 93, 94, 95]
for (const [index, issueNumber] of githubIssues.entries()) {
  const githubIssue = { repo: "withcoral/lagoon", number: issueNumber }
  const linearIssue = { id: "LIN-" + (489 + index), state: "triaged" }
  const updateInput = {
    issue_id: linearIssue.id,
    state_id: linearIssue.state,
    source: { provider: "github", issue: githubIssue.repo + "#" + githubIssue.number },
  }
  const options = { notifyAssignee: false, loopIndex: index + 1, total: githubIssues.length }`,
        currentOperation:
          '  await linear.issues.update(updateInput, options, "loop-iteration-" + (index + 1))',
        after: `  await audit.log({ action: "requested_loop_update", issue: linearIssue.id })
}`,
      },
      snippet: {
        before: `for (const [index, issueNumber] of githubIssues.entries()) {
  const updateInput = { issue_id: "LIN-491", source: { provider: "github", issue: "withcoral/lagoon#91" } }
  const options = { loopIndex: 2, total: 5 }`,
        currentOperation: '  await linear.issues.update(updateInput, options, "loop-iteration-2")',
        after: '}',
      },
    },
    provider: 'Linear',
    providerReference: 'Updates an issue using the authenticated Linear workspace identity.',
    rawInvocation: [
      {
        issue_id: 'LIN-491',
        state_id: 'triaged',
        note: 'Iteration 2/5: linked GitHub issue with matching approval-card feedback.',
        source: { provider: 'github', issue: 'withcoral/lagoon#91' },
      },
      { notifyAssignee: false, loopIndex: 2, total: 5 },
      'loop-iteration-2',
    ],
    invokingPrincipal: 'reef-agent',
    requestContext: {
      execIntent: 'Apply the prepared Linear updates from the submitted Program.',
      taskId: 'task_01K3Q9L7',
      taskIntent: 'Bring the tracked approval-review issues up to date.',
    },
    runContext: {
      runId: 'run_01K3Q8DA',
      status: 'running',
      workspace: 'Lagoon',
    },
    technicalDetails: [
      { label: 'Approval request', value: 'apr_01K3Q8L2 · pending · created 14:37 UTC' },
      { label: 'Operation invocation', value: 'inv_01K3Q8K6 · pending' },
      { label: 'Provider generation', value: 'linear@gen_0198' },
      { label: 'Event cursor', value: 'evt_00000052' },
      { label: 'Trace', value: '8f5c1f45b2ef4d65' },
    ],
  },
  savedFunction: {
    invocationArguments: [
      {
        label: 'Argument 1',
        value: {
          issue_number: 85,
          body: 'Linked Saved Function prepared this comment for the current run.',
          org: 'withcoral',
          repo: 'lagoon',
        },
      },
      {
        label: 'Argument 2',
        value: { format: 'markdown', functionPath: 'coral.functions.postApprovalFollowUp' },
      },
      { label: 'Argument 3', value: 'saved-function-call-01' },
    ],
    approvalAuthority: 'Workspace owner',
    expiresAt: '14:42 UTC',
    identity: 'coral-bot',
    operationCallPath: 'coral.providers.github.issues.createComment',
    policyText: 'Agents cannot approve their own operations.',
    programContext: {
      body: {
        before: `// Submitted Program body
await coral.functions.postApprovalFollowUp({ issueNumber: 85 })

// Linked Saved Function source snapshot: coral.functions.postApprovalFollowUp
export default async function postApprovalFollowUp(input: { issueNumber: number }) {
  const selectedIssue = { org: "withcoral", repo: "lagoon", number: input.issueNumber }
  const comment = {
    org: selectedIssue.org,
    repo: selectedIssue.repo,
    issue_number: selectedIssue.number,
    body: "Linked Saved Function prepared this comment for the current run.",
  }
  const options = { format: "markdown", functionPath: "coral.functions.postApprovalFollowUp" }`,
        currentOperation:
          '  await github.issues.createComment(comment, options, "saved-function-call-01")',
        after: `    return { issue: selectedIssue.number, status: "approval_requested" }
}`,
      },
      snippet: {
        before: `// Linked source: coral.functions.postApprovalFollowUp
export default async function postApprovalFollowUp(input) {
  const comment = { org: "withcoral", repo: "lagoon", issue_number: input.issueNumber }`,
        currentOperation:
          '  await github.issues.createComment(comment, { functionPath: "coral.functions.postApprovalFollowUp" }, "saved-function-call-01")',
        after: '}',
      },
    },
    provider: 'GitHub',
    providerReference:
      'Creates a comment on an issue in the selected repository using the authenticated GitHub account.',
    rawInvocation: [
      {
        issue_number: 85,
        body: 'Linked Saved Function prepared this comment for the current run.',
        org: 'withcoral',
        repo: 'lagoon',
      },
      { format: 'markdown', functionPath: 'coral.functions.postApprovalFollowUp' },
      'saved-function-call-01',
    ],
    invokingPrincipal: 'reef-agent',
    requestContext: {
      execIntent:
        'Call coral.functions.postApprovalFollowUp to prepare the linked GitHub follow-up.',
      taskId: 'task_01K3Q9S8',
      taskIntent: 'Post the approval-review follow-up using the linked Saved Function source.',
    },
    runContext: {
      runId: 'run_01K3Q8DA',
      status: 'running',
      workspace: 'Lagoon',
    },
    technicalDetails: [
      { label: 'Approval request', value: 'apr_01K3Q8S4 · pending · created 14:39 UTC' },
      { label: 'Operation invocation', value: 'inv_01K3Q8R9 · pending · linked Saved Function' },
      { label: 'Saved Function path', value: 'coral.functions.postApprovalFollowUp' },
      {
        label: 'Saved Function source',
        value: 'accepted Run snapshot · current edits cannot change it',
      },
      { label: 'Provider generation', value: 'github@gen_0198' },
      { label: 'Event cursor', value: 'evt_00000058' },
      { label: 'Trace', value: '8f5c1f45b2ef4d65' },
    ],
  },
}

function createArgumentRule({
  argumentIndex,
  formatObserved = formatEnvelopeObserved,
  label,
  path,
  policy,
  propose,
  validate,
}: {
  argumentIndex: number
  formatObserved?: (value: ArgumentValue) => string
  label: string
  path: string[]
  policy: string
  propose?: (value: ArgumentValue) => PolicyProposal
  validate: (value: ArgumentValue) => unknown
}): AuthorityEnvelopeRule {
  return {
    evaluate: (approval) => {
      const observed = readInvocationArgument(approval, argumentIndex, path)

      return observed === undefined
        ? {
            observed: 'Unavailable',
            proposal: {
              kind: 'unavailable',
              proposedPolicyChange: 'Cannot propose automatically',
              reason: 'The argument value must be available before changing Owner policy.',
            },
            status: 'unknown',
          }
        : {
            observed: formatObserved(observed),
            proposal: propose?.(observed),
            status: arkStatus(validate(observed)),
          }
    },
    label,
    policy,
  }
}

function stringEqualsRule(label: string, argumentIndex: number, path: string[], expected: string) {
  const schema = type('string').narrow((value) => value === expected)
  return createArgumentRule({
    argumentIndex,
    label,
    path,
    policy: `== ${JSON.stringify(expected)}`,
    propose: (observed) => automaticSetExpansion(label, [expected], observed),
    validate: (value) => schema(value),
  })
}

function stringInRule(label: string, argumentIndex: number, path: string[], allowed: string[]) {
  const schema = type('string').narrow((value) => allowed.includes(value))
  return createArgumentRule({
    argumentIndex,
    label,
    path,
    policy: `in ${JSON.stringify(allowed)}`,
    propose: (observed) => automaticSetExpansion(label, allowed, observed),
    validate: (value) => schema(value),
  })
}

function numberInRule(label: string, argumentIndex: number, path: string[], allowed: number[]) {
  const schema = type('number').narrow((value) => allowed.includes(value))
  return createArgumentRule({
    argumentIndex,
    label,
    path,
    policy: `in ${JSON.stringify(allowed)}`,
    propose: (observed) => automaticSetExpansion(label, allowed, observed),
    validate: (value) => schema(value),
  })
}

function booleanEqualsRule(
  label: string,
  argumentIndex: number,
  path: string[],
  expected: boolean,
) {
  const schema = type('boolean').narrow((value) => value === expected)
  return createArgumentRule({
    argumentIndex,
    label,
    path,
    policy: `== ${String(expected)}`,
    propose: () => ({
      kind: 'manual',
      proposedPolicyChange: 'Manual policy change required',
      reason: 'Changing a boolean policy can remove the constraint entirely.',
    }),
    validate: (value) => schema(value),
  })
}

function stringLengthRule(label: string, argumentIndex: number, path: string[], maximum: number) {
  const schema = type('string').narrow((value) => value.length <= maximum)
  return createArgumentRule({
    argumentIndex,
    formatObserved: (value) =>
      typeof value === 'string'
        ? `${value.length.toLocaleString()} characters`
        : JSON.stringify(value),
    label,
    path,
    policy: `length <= ${maximum.toLocaleString()}`,
    propose: () => ({
      kind: 'manual',
      proposedPolicyChange: 'Manual policy change required',
      reason: 'Size limits should be reviewed rather than expanded from one observed value.',
    }),
    validate: (value) => schema(value),
  })
}

function stringExcludesRule(
  label: string,
  argumentIndex: number,
  path: string[],
  excluded: string[],
) {
  const schema = type('string').narrow((value) =>
    excluded.every((candidate) => !value.includes(candidate)),
  )
  return createArgumentRule({
    argumentIndex,
    formatObserved: (value) => {
      if (typeof value !== 'string') return JSON.stringify(value)
      const matches = excluded.filter((candidate) => value.includes(candidate))
      return matches.length > 0 ? matches.join(', ') : 'No excluded values'
    },
    label,
    path,
    policy: `excludes ${JSON.stringify(excluded)}`,
    propose: () => ({
      kind: 'manual',
      proposedPolicyChange: 'Manual policy change required',
      reason: 'Excluded mentions are a semantic safety constraint and cannot auto-expand.',
    }),
    validate: (value) => schema(value),
  })
}

function stringArrayIncludesRule(
  label: string,
  argumentIndex: number,
  path: string[],
  required: string,
) {
  const schema = type('string[]').narrow((value) => value.includes(required))
  return createArgumentRule({
    argumentIndex,
    label,
    path,
    policy: `includes ${JSON.stringify(required)}`,
    propose: () => ({
      kind: 'manual',
      proposedPolicyChange: 'Manual policy change required',
      reason: 'Required-list constraints need explicit Owner review.',
    }),
    validate: (value) => schema(value),
  })
}

function automaticSetExpansion(
  label: string,
  allowed: Array<string | number>,
  observed: ArgumentValue,
): PolicyProposal {
  return {
    kind: 'automatic',
    proposedPolicyChange: `${label} in ${JSON.stringify([...allowed, observed])}`,
  }
}

function readInvocationArgument(
  approval: OperationApprovalModel,
  argumentIndex: number,
  path: string[],
): ArgumentValue | undefined {
  let value = approval.invocationArguments[argumentIndex]?.value

  for (const segment of path) {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) return undefined
    value = value[segment]
  }

  return value
}

function formatEnvelopeObserved(value: ArgumentValue) {
  return typeof value === 'string' ? value : JSON.stringify(value)
}

function createAuthorityEnvelope(
  envelopeId: string,
  approval: OperationApprovalModel,
  rules: AuthorityEnvelopeRule[],
): AuthorityEnvelope {
  return {
    envelopeId,
    evaluatedAt: '2026-09-02T12:00:00Z',
    expiresAt: '2026-09-30T23:59:00Z',
    installedBy: 'Workspace Owner',
    operationCallPath: approval.operationCallPath,
    rules,
  }
}

function createDataset(
  datasetKey: DatasetKey,
  insideRules: AuthorityEnvelopeRule[],
  outsideRules: AuthorityEnvelopeRule[],
): OperationApprovalDataset {
  const approval = approvalDatasets[datasetKey]
  return {
    approval,
    authorityEnvelopes: {
      inside: createAuthorityEnvelope(`env_${datasetKey}_inside`, approval, insideRules),
      outside: createAuthorityEnvelope(`env_${datasetKey}_outside`, approval, outsideRules),
    },
  }
}

const githubSharedRules = [
  stringEqualsRule('args[0].org', 0, ['org'], 'withcoral'),
  stringLengthRule('args[0].body.length', 0, ['body'], 2000),
  stringExcludesRule('args[0].body mentions', 0, ['body'], ['@channel', '@here']),
]
const linearSharedRules = [
  stringArrayIncludesRule('args[0].labels', 0, ['labels'], 'run-status'),
  booleanEqualsRule('args[1].notifyAssignee', 1, ['notifyAssignee'], true),
]
const loopSharedRules = [
  stringEqualsRule('args[0].source.issue', 0, ['source', 'issue'], 'withcoral/lagoon#91'),
  booleanEqualsRule('args[1].notifyAssignee', 1, ['notifyAssignee'], false),
]
const savedFunctionSharedRules = [
  stringEqualsRule('args[0].org', 0, ['org'], 'withcoral'),
  stringLengthRule('args[0].body.length', 0, ['body'], 2000),
  stringEqualsRule(
    'args[1].functionPath',
    1,
    ['functionPath'],
    'coral.functions.postApprovalFollowUp',
  ),
]

const datasets = {
  github: createDataset(
    'github',
    [
      ...githubSharedRules,
      stringEqualsRule('args[0].repo', 0, ['repo'], 'lagoon'),
      numberInRule('args[0].issue_number', 0, ['issue_number'], [85]),
    ],
    [
      ...githubSharedRules,
      stringEqualsRule('args[0].repo', 0, ['repo'], 'coral-internal'),
      numberInRule('args[0].issue_number', 0, ['issue_number'], [99]),
      stringEqualsRule('args[2]', 2, [], 'approval-card-review'),
    ],
  ),
  linear: createDataset(
    'linear',
    [
      ...linearSharedRules,
      stringInRule('args[0].issue_id', 0, ['issue_id'], ['LIN-482']),
      stringEqualsRule('args[0].state_id', 0, ['state_id'], 'in_review'),
    ],
    [
      ...linearSharedRules,
      stringInRule('args[0].issue_id', 0, ['issue_id'], ['LIN-999']),
      stringEqualsRule('args[0].state_id', 0, ['state_id'], 'done'),
    ],
  ),
  loop: createDataset(
    'loop',
    [
      ...loopSharedRules,
      stringInRule('args[0].issue_id', 0, ['issue_id'], ['LIN-491']),
      stringEqualsRule('args[0].state_id', 0, ['state_id'], 'triaged'),
    ],
    [
      ...loopSharedRules,
      stringInRule('args[0].issue_id', 0, ['issue_id'], ['LIN-999']),
      stringEqualsRule('args[0].state_id', 0, ['state_id'], 'done'),
    ],
  ),
  savedFunction: createDataset(
    'savedFunction',
    [
      ...savedFunctionSharedRules,
      stringEqualsRule('args[0].repo', 0, ['repo'], 'lagoon'),
      numberInRule('args[0].issue_number', 0, ['issue_number'], [85]),
    ],
    [
      ...savedFunctionSharedRules,
      stringEqualsRule('args[0].repo', 0, ['repo'], 'coral-internal'),
      numberInRule('args[0].issue_number', 0, ['issue_number'], [99]),
    ],
  ),
} satisfies Record<DatasetKey, OperationApprovalDataset>

export const Minimal: Story = {
  args: {
    compact: true,
    dataset: 'github',
  },
  parameters: {
    docs: {
      description: {
        story:
          'Minimal disclosure: selected dataset operation name plus provider, Approve, and Decline. No argument or Program evidence is shown.',
      },
    },
  },
}

export const MinimalWithExpiry: Story = {
  args: {
    compact: true,
    dataset: 'github',
    showExpiry: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Minimal disclosure with the stable operation name, provider, approval deadline, and decision actions.',
      },
    },
  },
}

export const MinimalWithRequester: Story = {
  args: {
    compact: true,
    dataset: 'github',
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Minimal disclosure with the stable operation name, provider, requesting principal, and decision actions.',
      },
    },
  },
}

export const MinimalWithRequesterAndExpiry: Story = {
  args: {
    compact: true,
    dataset: 'github',
    showExpiry: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Minimal disclosure with the stable operation name, provider, requesting principal, approval deadline, and decision actions.',
      },
    },
  },
}

export const Medium: Story = {
  args: {
    dataset: 'github',
    showArguments: true,
    showApprovalAuthority: true,
    showExpiry: true,
    showIdentity: true,
    showRequestMetadataInHeader: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Medium disclosure with the complete first-screen decision contract and exact positional arguments, without provider-specific interpretation.',
      },
    },
  },
}

export const MediumWithRequester: Story = {
  args: {
    dataset: 'github',
    showArguments: true,
    showApprovalAuthority: true,
    showExpiry: true,
    showIdentity: true,
    showRequestMetadataInHeader: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Earlier requester comparison, updated to preserve requester, approval authority, expiry, and exact positional arguments.',
      },
    },
  },
}

export const MediumWithExpiry: Story = {
  args: {
    dataset: 'github',
    showArguments: true,
    showApprovalAuthority: true,
    showExpiry: true,
    showIdentity: true,
    showRequestMetadataInHeader: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Earlier expiry comparison, updated to preserve requester, approval authority, expiry, and exact positional arguments.',
      },
    },
  },
}

export const MediumWithRequesterAndExpiry: Story = {
  args: {
    dataset: 'github',
    showArguments: true,
    showApprovalAuthority: true,
    showExpiry: true,
    showIdentity: true,
    showRequestMetadataInHeader: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Medium disclosure with requester, approval authority, and expiry in the header above exact positional arguments.',
      },
    },
  },
}

export const MediumExpandableExpanded: Story = {
  args: {
    argumentsCollapsible: true,
    argumentsInitiallyExpanded: true,
    dataset: 'github',
    showArguments: true,
    showApprovalAuthority: true,
    showExpiry: true,
    showIdentity: true,
    showRequestMetadataInHeader: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Safer expandable disclosure: requester and expiry sit in the header, while exact positional arguments start open.',
      },
    },
  },
}

export const MediumExpandableCollapsed: Story = {
  args: {
    argumentsCollapsible: true,
    argumentsInitiallyExpanded: false,
    dataset: 'github',
    showArguments: true,
    showApprovalAuthority: true,
    showExpiry: true,
    showIdentity: true,
    showRequestMetadataInHeader: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Collapsed-by-default experiment with requester and expiry in the header. Approval actions remain unchanged.',
      },
    },
  },
}

export const TaskIntentContext: Story = {
  args: {
    argumentsCollapsible: true,
    argumentsInitiallyExpanded: true,
    dataset: 'github',
    showApprovalAuthority: true,
    showArguments: true,
    showExpiry: true,
    showIdentity: true,
    showRequestContext: true,
    showRequestMetadataInHeader: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Medium+ disclosure with Task intent and MCP exec intent as request context. Exact invocation arguments remain the primary evidence.',
      },
    },
  },
}

export const EnvelopeMatches: Story = {
  args: {
    argumentsCollapsible: true,
    argumentsInitiallyExpanded: true,
    dataset: 'github',
    envelopeArgsDisplay: 'interleaved',
    envelopeMatch: 'inside',
    showArguments: true,
    showAuthorityEnvelopeMatch: true,
    showIdentity: true,
    showRequestMetadataInHeader: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'The exact Invocation passes every known check in a Workspace Owner-installed envelope and can continue without per-call approval.',
      },
    },
  },
}

export const EnvelopeDoesNotMatch: Story = {
  args: {
    argumentsCollapsible: true,
    argumentsInitiallyExpanded: true,
    dataset: 'github',
    envelopeArgsDisplay: 'interleaved',
    envelopeMatch: 'outside',
    showArguments: true,
    showApprovalAuthority: true,
    showAuthorityEnvelopeMatch: true,
    showExpiry: true,
    showIdentity: true,
    showRequestMetadataInHeader: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'The same Operation and invoking agent remain approval-required because exact checks fail or cannot be evaluated.',
      },
    },
  },
}

export const EnvelopePolicyUpdateProposal: Story = {
  args: {
    argumentsCollapsible: true,
    argumentsInitiallyExpanded: true,
    dataset: 'github',
    envelopeArgsDisplay: 'interleaved',
    envelopeMatch: 'outside',
    showArguments: true,
    showApprovalAuthority: true,
    showAuthorityEnvelopeMatch: true,
    showExpiry: true,
    showIdentity: true,
    showRequestMetadataInHeader: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Outside-envelope approval with a separate review step for bounded future Owner-policy changes. The proposal does not approve the pending Invocation.',
      },
    },
  },
}

export const ProgramSnippet: Story = {
  args: {
    argumentsCollapsible: true,
    argumentsInitiallyExpanded: true,
    dataset: 'github',
    showArguments: true,
    showApprovalAuthority: true,
    showExpiry: true,
    showIdentity: true,
    showProgramSnippet: true,
    showRequestMetadataInHeader: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Selected dataset plus one highlighted current operation call. The Program evidence is self-contained enough to show call location, not another argument table.',
      },
    },
  },
}

export const CollapsedProgramBody: Story = {
  args: {
    argumentsCollapsible: true,
    argumentsInitiallyExpanded: true,
    dataset: 'github',
    showArguments: true,
    showApprovalAuthority: true,
    showExpiry: true,
    showIdentity: true,
    showProgramBody: true,
    showRequestMetadataInHeader: true,
    showRequester: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Selected dataset with requester and expiry in the header plus a collapsed, self-contained Program body. The highlighted line identifies the current operation call when expanded.',
      },
    },
  },
}

export const MaximalEvidence: Story = {
  args: {
    argumentsCollapsible: true,
    argumentsInitiallyExpanded: true,
    dataset: 'github',
    showArguments: true,
    showApprovalAuthority: true,
    showIdentity: true,
    showProgramBody: true,
    showProgramSnippet: true,
    showProviderReference: true,
    showRequestMetadataInHeader: true,
    showRunContext: true,
    showTechnicalDetails: true,
  },
  parameters: {
    docs: {
      description: {
        story:
          'Maximal evidence for the selected dataset. Snippet, Program body, raw arguments, provider reference copy, and ids are available, but arguments remain the primary decision surface.',
      },
    },
  },
}

function OperationApprovalStory({ dataset, envelopeMatch, ...props }: OperationApprovalStoryProps) {
  const selectedDataset = datasets[dataset]

  return (
    <OperationApproval
      approval={selectedDataset.approval}
      authorityEnvelope={selectedDataset.authorityEnvelopes[envelopeMatch]}
      {...props}
    />
  )
}

function OperationApproval({
  approval,
  argumentsCollapsible,
  argumentsInitiallyExpanded,
  authorityEnvelope,
  compact,
  envelopeArgsDisplay = 'none',
  onApprove,
  onDecline,
  onUpdatePolicy,
  onViewRun,
  showArguments,
  showApprovalAuthority,
  showExpiry,
  showAuthorityEnvelopeMatch,
  showIdentity,
  showProgramBody,
  showProgramSnippet,
  showProviderReference,
  showRequestContext,
  showRequestMetadataInHeader,
  showRequester,
  showRunContext,
  showTechnicalDetails,
}: OperationApprovalProps) {
  const [policyProposalOpen, setPolicyProposalOpen] = useState(false)
  const hasDecisionContext = Boolean(showApprovalAuthority)
  const isEnvelopeVisible = showAuthorityEnvelopeMatch || envelopeArgsDisplay !== 'none'
  const envelopeEvaluation = isEnvelopeVisible
    ? evaluateAuthorityEnvelope(approval, authorityEnvelope)
    : undefined
  const canReviewFuturePolicy =
    isEnvelopeVisible && envelopeEvaluation?.decision === 'requiresApproval'
  const hasContext = Boolean(
    showArguments ||
    showExpiry ||
    showAuthorityEnvelopeMatch ||
    envelopeArgsDisplay !== 'none' ||
    showIdentity ||
    hasDecisionContext ||
    showProgramBody ||
    showProgramSnippet ||
    showProviderReference ||
    showRequestContext ||
    showRequester ||
    showRunContext ||
    showTechnicalDetails,
  )

  return (
    <section style={{ ...cardStyle, ...(compact ? compactCardStyle : {}) }}>
      <ApprovalHeader
        approval={approval}
        envelopeEvaluation={envelopeEvaluation}
        hasContext={hasContext}
        showApprovalAuthority={showApprovalAuthority}
        showExpiry={showExpiry}
        showIdentity={showIdentity}
        showRequestMetadata={showRequestMetadataInHeader}
        showRequester={showRequester}
      />

      {showArguments ? (
        <InvocationArgumentsSection
          collapsible={argumentsCollapsible}
          envelopeChecks={envelopeArgsDisplay !== 'none' ? envelopeEvaluation?.checks : undefined}
          envelopeDisplay={envelopeArgsDisplay}
          initiallyExpanded={argumentsInitiallyExpanded}
          invocationArguments={approval.invocationArguments}
        />
      ) : null}

      {showAuthorityEnvelopeMatch && envelopeEvaluation ? (
        <EnvelopeCheckSection evaluation={envelopeEvaluation} />
      ) : null}

      {(showRequestContext && approval.requestContext) ||
      (showProgramSnippet && approval.programContext) ? (
        <ContextSection
          programSnippet={showProgramSnippet ? approval.programContext?.snippet : undefined}
          requestContext={showRequestContext ? approval.requestContext : undefined}
        />
      ) : null}

      {showProgramBody && approval.programContext ? (
        <ProgramBodyDisclosure programBody={approval.programContext.body} />
      ) : null}

      {(showRequester || showExpiry) && !hasDecisionContext && !showRequestMetadataInHeader ? (
        <dl style={decisionDetailsStyle}>
          {showRequester ? (
            <DetailRow
              label="Requested by"
              showDivider={showExpiry}
              value={approval.invokingPrincipal}
            />
          ) : null}
          {showExpiry ? (
            <DetailRow label="Expiry" showDivider={false} value={`Expires ${approval.expiresAt}`} />
          ) : null}
        </dl>
      ) : null}

      {hasDecisionContext && !showRequestMetadataInHeader ? (
        <section style={decisionContextStyle}>
          <dl style={decisionDetailsStyle}>
            {!showRequestMetadataInHeader ? (
              <DetailRow label="Requested by" value={approval.invokingPrincipal} />
            ) : null}
            <DetailRow label="Can approve" value={approval.approvalAuthority} />
            {!showRequestMetadataInHeader ? (
              <DetailRow
                label="Expiry"
                showDivider={false}
                value={`Expires ${approval.expiresAt}`}
              />
            ) : null}
          </dl>
          <Typography.BodySmall as="p" style={zeroMarginStyle} variant="tertiary">
            {approval.policyText}
          </Typography.BodySmall>
        </section>
      ) : null}

      {showProviderReference && approval.providerReference ? (
        <ProviderReferenceSection providerReference={approval.providerReference} />
      ) : null}

      {showTechnicalDetails ? <TechnicalDetailsSection approval={approval} /> : null}

      {showRunContext ? (
        <div style={runContextStyle}>
          <div style={runCopyStyle}>
            <Typography.BodySmallStrong as="p" style={zeroMarginStyle}>
              Run {approval.runContext.runId}
            </Typography.BodySmallStrong>
            <Typography.BodySmall as="p" style={zeroMarginStyle} variant="tertiary">
              {approval.runContext.status} · {approval.runContext.workspace}
            </Typography.BodySmall>
          </div>
          <Button.Container onClick={onViewRun} size="22" variant="bare">
            <Button.Text>View run</Button.Text>
            <Button.Icon name="ArrowUpRight" />
          </Button.Container>
        </div>
      ) : null}

      {envelopeEvaluation?.decision === 'allow' ? null : (
        <ApprovalActions
          approveLabel={isEnvelopeVisible ? 'Approve once' : 'Approve'}
          onApprove={onApprove}
          onDecline={onDecline}
          onReviewPolicy={canReviewFuturePolicy ? () => setPolicyProposalOpen(true) : undefined}
        />
      )}

      {canReviewFuturePolicy && envelopeEvaluation ? (
        <PolicyUpdateProposalDialog
          evaluation={envelopeEvaluation}
          onOpenChange={setPolicyProposalOpen}
          onUpdatePolicy={onUpdatePolicy}
          open={policyProposalOpen}
        />
      ) : null}
    </section>
  )
}

function ApprovalHeader({
  approval,
  envelopeEvaluation,
  hasContext,
  showApprovalAuthority,
  showExpiry,
  showIdentity,
  showRequestMetadata,
  showRequester,
}: {
  approval: OperationApprovalModel
  envelopeEvaluation?: AuthorityEnvelopeEvaluation
  hasContext: boolean
  showApprovalAuthority?: boolean
  showExpiry?: boolean
  showIdentity?: boolean
  showRequestMetadata?: boolean
  showRequester?: boolean
}) {
  const requestMetadata = [
    (showRequester || showApprovalAuthority) && `Requested by: ${approval.invokingPrincipal}`,
    showApprovalAuthority && `Can approve: ${approval.approvalAuthority}`,
    (showExpiry || showApprovalAuthority) && `Expires ${approval.expiresAt}`,
  ].filter(Boolean)

  return (
    <div style={headerStyle}>
      <span style={operationIconStyle}>
        <Icon color="warning" name="ShieldAlert" size="18" />
      </span>
      <div style={titleGroupStyle}>
        <Typography.BodyLargeStrong as="h2">
          {approval.operationCallPath}
        </Typography.BodyLargeStrong>
        <Typography.BodySmall as="p" style={zeroMarginStyle} variant="tertiary">
          {[`Provider: ${approval.provider}`, showIdentity && `Identity: ${approval.identity}`]
            .filter(Boolean)
            .join(' · ')}
        </Typography.BodySmall>
        {showRequestMetadata && requestMetadata.length > 0 ? (
          <Typography.BodySmall as="p" style={zeroMarginStyle} variant="tertiary">
            {requestMetadata.join(' · ')}
          </Typography.BodySmall>
        ) : null}
      </div>
      {envelopeEvaluation?.decision === 'allow' ? (
        <Pill color="green">Allowed by Owner policy</Pill>
      ) : envelopeEvaluation ? (
        <Pill color="amber">Outside Owner policy</Pill>
      ) : hasContext ? (
        <Pill color="amber">Approval required</Pill>
      ) : null}
    </div>
  )
}

function InvocationArgumentsSection({
  collapsible,
  envelopeChecks,
  envelopeDisplay,
  initiallyExpanded,
  invocationArguments,
}: {
  collapsible?: boolean
  envelopeChecks?: AuthorityEnvelopeEvaluation['checks']
  envelopeDisplay: EnvelopeArgsDisplay
  initiallyExpanded?: boolean
  invocationArguments: OperationApprovalModel['invocationArguments']
}) {
  const argumentsId = useId()
  const [expanded, setExpanded] = useState(Boolean(initiallyExpanded))

  return (
    <section style={sectionStyle}>
      {collapsible ? (
        <Button.Container
          aria-controls={argumentsId}
          aria-expanded={expanded}
          fullWidth
          onClick={() => setExpanded((current) => !current)}
          size="22"
          style={argumentsDisclosureStyle}
          variant="bare"
        >
          <Button.Icon name={expanded ? 'ChevronDown' : 'ChevronRight'} />
          <Typography.BodySmallStrong variant="tertiary">
            Arguments ({invocationArguments.length})
          </Typography.BodySmallStrong>
        </Button.Container>
      ) : (
        <Typography.BodySmallStrong as="h3" style={sectionHeadingStyle} variant="tertiary">
          Arguments
        </Typography.BodySmallStrong>
      )}
      {!collapsible || expanded ? (
        <dl id={argumentsId} style={detailsStyle}>
          {invocationArguments.map(({ label, value }, index) => (
            <InvocationArgumentRow
              annotations={envelopeChecks?.filter(({ label: checkLabel }) =>
                checkLabel.startsWith(`args[${index}]`),
              )}
              argumentIndex={index}
              envelopeDisplay={envelopeDisplay}
              key={label}
              label={label}
              showDivider={index < invocationArguments.length - 1}
              value={value}
            />
          ))}
        </dl>
      ) : null}
    </section>
  )
}

function InvocationArgumentRow({
  annotations,
  argumentIndex,
  envelopeDisplay,
  label,
  showDivider,
  value,
}: {
  annotations?: AuthorityEnvelopeEvaluation['checks']
  argumentIndex: number
  envelopeDisplay: EnvelopeArgsDisplay
  label: string
  showDivider: boolean
  value: ArgumentValue
}) {
  const argumentStatus = aggregateEnvelopeStatus(annotations)
  const isInterleavedObject = envelopeDisplay === 'interleaved' && isArgumentRecord(value)

  return (
    <div
      style={{
        ...argumentGroupStyle,
        ...(argumentStatus && !isInterleavedObject
          ? envelopeArgumentStatusStyles[argumentStatus]
          : {}),
        ...(!showDivider ? lastDetailRowStyle : {}),
      }}
    >
      {isInterleavedObject ? (
        <InterleavedArgumentFields
          annotations={annotations}
          argumentIndex={argumentIndex}
          argumentLabel={label}
          value={value}
        />
      ) : (
        <DetailRow label={label} showDivider={false} value={value} />
      )}
      {envelopeDisplay !== 'interleaved' && annotations && annotations.length > 0 ? (
        <div style={argumentAnnotationsStyle}>
          {annotations.map((annotation) => (
            <EnvelopeArgumentAnnotation annotation={annotation} key={annotation.label} />
          ))}
        </div>
      ) : null}
      {envelopeDisplay === 'interleaved' && !isInterleavedObject && annotations?.length ? (
        <div style={argumentAnnotationsStyle}>
          {annotations.map((annotation) => (
            <EnvelopeArgumentAnnotation annotation={annotation} key={annotation.label} />
          ))}
        </div>
      ) : null}
    </div>
  )
}

function InterleavedArgumentFields({
  annotations,
  argumentIndex,
  argumentLabel,
  value,
}: {
  annotations?: AuthorityEnvelopeEvaluation['checks']
  argumentIndex: number
  argumentLabel: string
  value: Record<string, ArgumentValue>
}) {
  return (
    <div style={interleavedArgumentStyle}>
      <Typography.BodySmall as="p" style={interleavedArgumentLabelStyle} variant="tertiary">
        {argumentLabel}
      </Typography.BodySmall>
      <div>
        {Object.entries(value).map(([field, fieldValue], index, fields) => {
          const fieldAnnotations = annotations?.filter(({ label }) =>
            label.startsWith(`args[${argumentIndex}].${field}`),
          )
          const fieldStatus = aggregateEnvelopeStatus(fieldAnnotations)

          return (
            <div
              key={field}
              style={{
                ...interleavedFieldStyle,
                ...(fieldStatus ? envelopeArgumentStatusStyles[fieldStatus] : {}),
                ...(index === fields.length - 1 ? lastDetailRowStyle : {}),
              }}
            >
              <div style={interleavedFieldValueStyle}>
                <Typography.BodySmallStrong as="span" variant="tertiary">
                  {field}
                </Typography.BodySmallStrong>
                <Typography.BodySmall as="div" style={detailValueStyle} variant="primary">
                  {renderArgumentValue(fieldValue)}
                </Typography.BodySmall>
              </div>
              {fieldAnnotations?.map((annotation) => (
                <EnvelopeArgumentAnnotation annotation={annotation} key={annotation.label} />
              ))}
            </div>
          )
        })}
      </div>
    </div>
  )
}

function EnvelopeArgumentAnnotation({
  annotation: { label, policy, status },
}: {
  annotation: AuthorityEnvelopeEvaluation['checks'][number]
}) {
  return (
    <div style={argumentAnnotationStyle}>
      <code style={policyExpressionStyle}>
        Policy: {label} {policy}
      </code>
      <Pill color={envelopeStatusColors[status]}>{envelopeStatusLabels[status]}</Pill>
    </div>
  )
}

function isArgumentRecord(value: ArgumentValue): value is Record<string, ArgumentValue> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function aggregateEnvelopeStatus(
  annotations: AuthorityEnvelopeEvaluation['checks'] | undefined,
): EnvelopeCheckStatus | undefined {
  if (!annotations || annotations.length === 0) return undefined
  if (annotations.some(({ status }) => status === 'fail')) return 'fail'
  if (annotations.some(({ status }) => status === 'unknown')) return 'unknown'
  return 'pass'
}

function EnvelopeCheckSection({ evaluation }: { evaluation: AuthorityEnvelopeEvaluation }) {
  const matches = evaluation.decision === 'allow'

  return (
    <section style={sectionStyle}>
      <Typography.BodySmallStrong as="h3" style={sectionHeadingStyle} variant="tertiary">
        Envelope check
      </Typography.BodySmallStrong>
      <div style={envelopeResultStyle}>
        <div style={envelopeResultCopyStyle}>
          <Typography.BodySmallStrong as="p" style={zeroMarginStyle}>
            {matches ? 'Allowed by Owner policy' : 'Outside Owner policy'}
          </Typography.BodySmallStrong>
          <Typography.BodySmall as="p" style={zeroMarginStyle} variant="secondary">
            {matches
              ? 'Can continue without per-call approval'
              : 'Requires owner approval for this Invocation'}
          </Typography.BodySmall>
        </div>
        <Pill color={matches ? 'green' : 'amber'}>{matches ? 'Allowed' : 'Review'}</Pill>
      </div>
      <dl style={contextDetailsStyle}>
        <DetailRow label="Installed by" value={evaluation.installedBy} />
        <DetailRow label="Envelope expiry" value={evaluation.expiresAt} />
        {evaluation.checks.map(({ label, observed, policy, status }, index) => (
          <EnvelopeCheckRow
            key={label}
            label={label}
            observed={observed}
            policy={policy}
            showDivider={index < evaluation.checks.length - 1}
            status={status}
          />
        ))}
      </dl>
      <Typography.BodySmall as="p" style={zeroMarginStyle} variant="tertiary">
        {matches
          ? `Allowed by envelope ${evaluation.envelopeId}`
          : `Envelope ${evaluation.envelopeId} did not authorize this Invocation`}
      </Typography.BodySmall>
    </section>
  )
}

function PolicyUpdateProposalDialog({
  evaluation,
  onOpenChange,
  onUpdatePolicy,
  open,
}: {
  evaluation: AuthorityEnvelopeEvaluation
  onOpenChange: (open: boolean) => void
  onUpdatePolicy: () => void
  open: boolean
}) {
  const proposedChecks = evaluation.checks.filter(({ status }) => status !== 'pass')
  const canUpdatePolicy = proposedChecks.every(({ proposal }) => proposal?.kind === 'automatic')

  return (
    <Dialog.Root onOpenChange={onOpenChange} open={open}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="l">
          <Dialog.Title>Allow similar future calls</Dialog.Title>
          <Dialog.Description>
            This proposes an Owner policy update for envelope {evaluation.envelopeId}. It does not
            approve this invocation.
          </Dialog.Description>
          <Dialog.Close />
          <section style={policyProposalListStyle}>
            {proposedChecks.map(({ label, observed, policy, proposal, status }, index) => (
              <PolicyUpdateProposalRow
                key={label}
                label={label}
                observed={observed}
                policy={policy}
                proposal={proposal}
                showDivider={index < proposedChecks.length - 1}
                status={status}
              />
            ))}
          </section>
          {!canUpdatePolicy ? (
            <Typography.BodySmall
              as="p"
              style={{ ...zeroMarginStyle, color: theme.content.warning }}
              variant="secondary"
            >
              Resolve manual or unavailable checks before updating Owner policy.
            </Typography.BodySmall>
          ) : null}
          <Dialog.Actions>
            <Button.TextButton onClick={() => onOpenChange(false)} variant="secondary">
              Cancel
            </Button.TextButton>
            <Button.TextButton
              disabled={!canUpdatePolicy}
              onClick={() => {
                onUpdatePolicy()
                onOpenChange(false)
              }}
            >
              Update policy
            </Button.TextButton>
          </Dialog.Actions>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function PolicyUpdateProposalRow({
  label,
  observed,
  policy,
  proposal,
  showDivider,
  status,
}: {
  label: string
  observed: string
  policy: string
  proposal?: PolicyProposal
  showDivider: boolean
  status: EnvelopeCheckStatus
}) {
  const resolvedProposal =
    proposal ??
    ({
      kind: 'unavailable',
      proposedPolicyChange: 'Cannot propose automatically',
      reason: 'This check is not a mechanically expandable argument filter.',
    } satisfies PolicyProposal)

  return (
    <div style={{ ...policyProposalRowStyle, ...(!showDivider ? lastDetailRowStyle : {}) }}>
      <div style={policyProposalHeadingStyle}>
        <Typography.BodySmallStrong as="h3">{label}</Typography.BodySmallStrong>
        <Pill color={envelopeStatusColors[status]}>{envelopeStatusLabels[status]}</Pill>
      </div>
      <Typography.BodySmall as="p" style={policyObservedStyle} variant="secondary">
        Observed: {observed}
      </Typography.BodySmall>
      <PolicyProposalDiff currentPolicy={`${label} ${policy}`} proposal={resolvedProposal} />
    </div>
  )
}

function PolicyProposalDiff({
  currentPolicy,
  proposal,
}: {
  currentPolicy: string
  proposal: PolicyProposal
}) {
  return (
    <div aria-label="Proposed policy diff" style={policyDiffStyle}>
      {proposal.kind === 'automatic' ? (
        <>
          <code style={{ ...policyDiffLineStyle, ...policyDiffRemovedStyle }}>
            - {currentPolicy}
          </code>
          <code style={{ ...policyDiffLineStyle, ...policyDiffAddedStyle }}>
            + {proposal.proposedPolicyChange}
          </code>
        </>
      ) : (
        <>
          <code style={policyDiffLineStyle}> {currentPolicy}</code>
          <code style={{ ...policyDiffLineStyle, ...policyDiffCommentStyle }}>
            # {proposal.proposedPolicyChange}
            {proposal.reason ? `: ${proposal.reason}` : ''}
          </code>
        </>
      )}
    </div>
  )
}

function EnvelopeCheckRow({
  label,
  observed,
  policy,
  showDivider,
  status,
}: {
  label: string
  observed: string
  policy: string
  showDivider: boolean
  status: EnvelopeCheckStatus
}) {
  return (
    <div style={{ ...detailRowStyle, ...(!showDivider ? lastDetailRowStyle : {}) }}>
      <Typography.BodySmall as="dt" variant="tertiary">
        {label}
      </Typography.BodySmall>
      <div style={envelopeCheckValueStyle}>
        <div style={envelopeCheckCopyStyle}>
          <Typography.BodySmall as="dd" style={detailValueStyle} variant="secondary">
            Observed: {observed}
          </Typography.BodySmall>
          <code style={policyExpressionStyle}>Policy: {policy}</code>
        </div>
        <Pill color={envelopeStatusColors[status]}>{envelopeStatusLabels[status]}</Pill>
      </div>
    </div>
  )
}

function ContextSection({
  programSnippet,
  requestContext,
}: {
  programSnippet?: ProgramEvidence
  requestContext?: RequestContext
}) {
  return (
    <section style={sectionStyle}>
      <Typography.BodySmallStrong as="h3" style={sectionHeadingStyle} variant="tertiary">
        Context
      </Typography.BodySmallStrong>
      {requestContext ? <RequestContextDetails requestContext={requestContext} /> : null}
      {programSnippet ? <ProgramEvidenceBlock evidence={programSnippet} /> : null}
    </section>
  )
}

function RequestContextDetails({ requestContext }: { requestContext: RequestContext }) {
  return (
    <dl style={contextDetailsStyle}>
      <DetailRow label="Task" value={requestContext.taskId} />
      <DetailRow label="Task intent" value={requestContext.taskIntent} />
      <DetailRow label="Exec intent" showDivider={false} value={requestContext.execIntent} />
    </dl>
  )
}

function ProgramBodyDisclosure({ programBody }: { programBody: ProgramEvidence }) {
  return (
    <details open={false} style={disclosureSectionStyle}>
      <Typography.BodySmallStrong as="summary" style={disclosureSummaryStyle}>
        Program body
      </Typography.BodySmallStrong>
      <div style={disclosureContentStyle}>
        <ProgramEvidenceBlock evidence={programBody} />
      </div>
    </details>
  )
}

function ProviderReferenceSection({ providerReference }: { providerReference: string }) {
  return (
    <details style={disclosureSectionStyle}>
      <Typography.BodySmallStrong as="summary" style={disclosureSummaryStyle}>
        Reference
      </Typography.BodySmallStrong>
      <div style={disclosureContentStyle}>
        <Typography.BodySmall as="p" style={zeroMarginStyle} variant="tertiary">
          Optional provider-authored text; it does not define this approval’s exact effect.
        </Typography.BodySmall>
        <Typography.BodySmall as="p" style={zeroMarginStyle} variant="secondary">
          {providerReference}
        </Typography.BodySmall>
      </div>
    </details>
  )
}

function TechnicalDetailsSection({ approval }: { approval: OperationApprovalModel }) {
  const technicalDetails = buildTechnicalDetails(approval)

  return (
    <details style={disclosureSectionStyle}>
      <Typography.BodySmallStrong as="summary" style={disclosureSummaryStyle}>
        Technical details
      </Typography.BodySmallStrong>
      <dl style={technicalListStyle}>
        {technicalDetails.map(({ label, value }, index) => (
          <DetailRow
            key={label}
            label={label}
            showDivider={index < technicalDetails.length - 1}
            value={value}
          />
        ))}
      </dl>
    </details>
  )
}

function ApprovalActions({
  approveLabel = 'Approve',
  onApprove,
  onDecline,
  onReviewPolicy,
}: {
  approveLabel?: string
  onApprove: () => void
  onDecline: () => void
  onReviewPolicy?: () => void
}) {
  return (
    <div style={actionFooterStyle}>
      {onReviewPolicy ? (
        <Button.Container onClick={onReviewPolicy} size="32" variant="secondary">
          <Button.Text>Allow similar future calls…</Button.Text>
        </Button.Container>
      ) : null}
      <div style={actionsStyle}>
        <Button.Container onClick={onDecline} size="32" variant="secondary">
          <Button.Text>Decline</Button.Text>
        </Button.Container>
        <Button.Container onClick={onApprove} size="32" variant="primary">
          <Button.Text>{approveLabel}</Button.Text>
        </Button.Container>
      </div>
    </div>
  )
}

function buildTechnicalDetails(approval: OperationApprovalModel) {
  return [
    { label: 'Operation call path', value: approval.operationCallPath },
    { label: 'Identity', value: `${approval.provider} · ${approval.identity}` },
    { label: 'Raw invocation', value: JSON.stringify(approval.rawInvocation, null, 2) },
    ...approval.technicalDetails,
  ]
}

function ProgramEvidenceBlock({ evidence }: { evidence: ProgramEvidence }) {
  return (
    <pre style={codeBlockStyle}>
      {evidence.before ? `${evidence.before}\n` : null}
      <mark style={currentOperationStyle}>{evidence.currentOperation}</mark>
      {evidence.after ? `\n${evidence.after}` : null}
    </pre>
  )
}

function DetailRow({
  label,
  showDivider = true,
  value,
}: {
  label: string
  showDivider?: boolean
  value: ArgumentValue
}) {
  return (
    <div style={{ ...detailRowStyle, ...(!showDivider ? lastDetailRowStyle : {}) }}>
      <Typography.BodySmall as="dt" variant="tertiary">
        {label}
      </Typography.BodySmall>
      <Typography.BodySmall as="dd" style={detailValueStyle} variant="primary">
        {renderArgumentValue(value)}
      </Typography.BodySmall>
    </div>
  )
}

function renderArgumentValue(value: ArgumentValue) {
  if (typeof value === 'object' && value !== null) {
    return <pre style={nestedValueStyle}>{JSON.stringify(value, null, 2)}</pre>
  }

  return String(value)
}

const storyCanvasStyle: CSSProperties = {
  alignItems: 'flex-start',
  background: theme.surface.main,
  boxSizing: 'border-box',
  display: 'flex',
  justifyContent: 'center',
  padding: '20px',
}

const cardStyle: CSSProperties = {
  background: theme.surface.card,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: '12px',
  boxSizing: 'border-box',
  display: 'flex',
  flexDirection: 'column',
  gap: '16px',
  maxWidth: '720px',
  padding: '16px',
  width: '100%',
}

const compactCardStyle: CSSProperties = { gap: '12px', maxWidth: '560px' }

const headerStyle: CSSProperties = {
  alignItems: 'flex-start',
  display: 'flex',
  gap: '12px',
  minWidth: 0,
}

const operationIconStyle: CSSProperties = {
  alignItems: 'center',
  background: theme.content.warningBackground,
  borderRadius: '8px',
  display: 'flex',
  flex: '0 0 auto',
  height: '32px',
  justifyContent: 'center',
  width: '32px',
}

const titleGroupStyle: CSSProperties = {
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: '2px',
  minWidth: 0,
}

const zeroMarginStyle: CSSProperties = { margin: 0 }

const sectionStyle: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  gap: '6px',
}

const sectionHeadingStyle: CSSProperties = {
  borderBottom: `1px solid ${theme.stroke.secondary}`,
  margin: 0,
  paddingBottom: '6px',
}

const argumentsDisclosureStyle: CSSProperties = {
  ...sectionHeadingStyle,
  alignItems: 'center',
  background: 'transparent',
  border: 0,
  borderBottom: `1px solid ${theme.stroke.secondary}`,
  cursor: 'pointer',
  display: 'flex',
  gap: '4px',
  justifyContent: 'flex-start',
  padding: '0 0 6px',
  textAlign: 'left',
  width: '100%',
}

const contextDetailsStyle: CSSProperties = { margin: 0 }

const codeBlockStyle: CSSProperties = {
  background: theme.surface.main,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: '8px',
  color: theme.content.secondary,
  fontFamily: 'monospace',
  fontSize: '12px',
  lineHeight: 1.5,
  margin: 0,
  overflowX: 'auto',
  padding: '10px 12px',
  whiteSpace: 'pre-wrap',
}

const currentOperationStyle: CSSProperties = {
  background: theme.content.warningBackground,
  borderRadius: '4px',
  color: theme.content.primary,
  padding: '1px 3px',
}

const detailsStyle: CSSProperties = { margin: 0 }

const detailRowStyle: CSSProperties = {
  alignItems: 'baseline',
  borderBottom: `1px solid ${theme.stroke.secondary}`,
  display: 'grid',
  gap: '16px',
  gridTemplateColumns: '140px minmax(0, 1fr)',
  padding: '8px 0',
}

const argumentGroupStyle: CSSProperties = {
  borderBottom: `1px solid ${theme.stroke.secondary}`,
}

const interleavedArgumentStyle: CSSProperties = {
  display: 'grid',
  gap: '16px',
  gridTemplateColumns: '140px minmax(0, 1fr)',
  padding: '8px 0',
}

const interleavedArgumentLabelStyle: CSSProperties = {
  margin: 0,
  paddingTop: '8px',
}

const interleavedFieldStyle: CSSProperties = {
  borderBottom: `1px solid ${theme.stroke.secondary}`,
  display: 'flex',
  flexDirection: 'column',
  gap: '4px',
  padding: '8px',
}

const interleavedFieldValueStyle: CSSProperties = {
  alignItems: 'baseline',
  display: 'grid',
  gap: '12px',
  gridTemplateColumns: '120px minmax(0, 1fr)',
}

const envelopeArgumentStatusStyles: Record<EnvelopeCheckStatus, CSSProperties> = {
  fail: {
    borderLeft: `1px solid ${theme.content.error}`,
    paddingLeft: '8px',
  },
  pass: {
    borderLeft: `1px solid ${theme.content.success}`,
    paddingLeft: '8px',
  },
  unknown: {
    borderLeft: `1px solid ${theme.content.warning}`,
    paddingLeft: '8px',
  },
}

const argumentAnnotationsStyle: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  gap: '4px',
  marginLeft: '156px',
}

const argumentAnnotationStyle: CSSProperties = {
  alignItems: 'center',
  display: 'flex',
  gap: '8px',
  justifyContent: 'space-between',
  minWidth: 0,
}

const policyExpressionStyle: CSSProperties = {
  color: theme.content.tertiary,
  fontFamily: 'monospace',
  fontSize: '11px',
  lineHeight: 1.4,
  overflowWrap: 'anywhere',
}

const lastDetailRowStyle: CSSProperties = { borderBottom: 0 }

const detailValueStyle: CSSProperties = {
  margin: 0,
  minWidth: 0,
  overflowWrap: 'anywhere',
}

const envelopeResultStyle: CSSProperties = {
  alignItems: 'center',
  display: 'flex',
  gap: '12px',
  justifyContent: 'space-between',
  padding: '2px 0 6px',
}

const envelopeResultCopyStyle: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  gap: '2px',
  minWidth: 0,
}

const envelopeCheckValueStyle: CSSProperties = {
  alignItems: 'center',
  display: 'flex',
  gap: '8px',
  justifyContent: 'space-between',
  minWidth: 0,
}

const envelopeCheckCopyStyle: CSSProperties = {
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: '2px',
  minWidth: 0,
}

const policyProposalListStyle: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  marginTop: '16px',
}

const policyProposalRowStyle: CSSProperties = {
  borderBottom: `1px solid ${theme.stroke.secondary}`,
  padding: '12px 0',
}

const policyProposalHeadingStyle: CSSProperties = {
  alignItems: 'center',
  display: 'flex',
  gap: '12px',
  justifyContent: 'space-between',
}

const policyObservedStyle: CSSProperties = {
  margin: '4px 0 8px',
}

const policyDiffStyle: CSSProperties = {
  background: theme.surface.main,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: '8px',
  overflow: 'hidden',
}

const policyDiffLineStyle: CSSProperties = {
  color: theme.content.secondary,
  display: 'block',
  fontFamily: 'monospace',
  fontSize: '11px',
  lineHeight: 1.5,
  overflowWrap: 'anywhere',
  padding: '5px 8px',
  whiteSpace: 'pre-wrap',
}

const policyDiffRemovedStyle: CSSProperties = {
  background: theme.content.errorBackground,
  color: theme.content.error,
}

const policyDiffAddedStyle: CSSProperties = {
  background: theme.content.successBackground,
  color: theme.content.success,
}

const policyDiffCommentStyle: CSSProperties = {
  background: theme.content.warningBackground,
  color: theme.content.warning,
}

const nestedValueStyle: CSSProperties = {
  ...codeBlockStyle,
  fontSize: '11px',
  maxWidth: '100%',
}

const decisionContextStyle: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
}

const decisionDetailsStyle: CSSProperties = {
  borderTop: `1px solid ${theme.stroke.secondary}`,
  margin: 0,
}

const disclosureSectionStyle: CSSProperties = { margin: 0 }

const disclosureSummaryStyle: CSSProperties = {
  ...sectionHeadingStyle,
  color: theme.content.secondary,
  cursor: 'pointer',
}

const disclosureContentStyle: CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
  paddingTop: '8px',
}

const technicalListStyle: CSSProperties = { margin: '8px 0 0' }

const runContextStyle: CSSProperties = {
  alignItems: 'center',
  background: theme.surface.mainContent,
  borderRadius: '8px',
  display: 'flex',
  gap: '12px',
  justifyContent: 'space-between',
  padding: '10px 12px',
}

const runCopyStyle: CSSProperties = {
  display: 'flex',
  flex: '1 1 auto',
  flexDirection: 'column',
  gap: '2px',
  minWidth: 0,
}

const actionsStyle: CSSProperties = {
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: '8px',
  justifyContent: 'flex-end',
}

const actionFooterStyle: CSSProperties = {
  alignItems: 'flex-end',
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
}
