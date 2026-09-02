import type { CSSProperties } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'

import { type } from 'arktype'
import { useId, useState } from 'react'
import { fn } from 'storybook/test'

import { Button, Typography } from '@/wax/components'
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
type DatasetKey =
  | 'envelopeMatches'
  | 'envelopeOutside'
  | 'github'
  | 'linear'
  | 'loop'
  | 'savedFunction'

interface OperationApprovalStoryProps {
  argumentsCollapsible?: boolean
  argumentsInitiallyExpanded?: boolean
  compact?: boolean
  dataset: DatasetKey
  onApprove: () => void
  onDecline: () => void
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

type OperationApprovalProps = Omit<OperationApprovalStoryProps, 'dataset'> & {
  approval: OperationApprovalModel
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

interface AuthorityEnvelopeEvaluation {
  checks: Array<{
    label: string
    observed: string
    policy: string
    status: EnvelopeCheckStatus
  }>
  decision: 'allow' | 'requiresApproval'
  envelopeId: string
  expiresAt: string
  installedBy: string
}

interface AuthorityEnvelope {
  envelopeId: string
  facts: {
    body: string
    commentsToday?: number
    evaluatedAt: string
    issueState?: string
    operationCallPath: string
    repository: string
  }
  installedBy: string
  policy: {
    allowedOperationCallPath: string
    allowedRepository: string
    dailyCommentLimit: number
    expiresAt: string
    forbiddenMassMentions: string[]
    maxBodyCharacters: number
    requiredIssueState: string
  }
}

interface OperationApprovalModel {
  approvalAuthority: string
  authorityEnvelope?: AuthorityEnvelope
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
    dataset: {
      control: 'select',
      options: ['github', 'linear', 'loop', 'savedFunction', 'envelopeMatches', 'envelopeOutside'],
    },
    showAuthorityEnvelopeMatch: {
      control: 'boolean',
      description: 'Execute and show a Storybook-only deterministic Owner-policy envelope.',
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
    dataset: 'github',
    onApprove: fn(),
    onDecline: fn(),
    onViewRun: fn(),
    showRequestContext: false,
    showProviderReference: false,
    showAuthorityEnvelopeMatch: false,
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

The envelope stories execute Storybook-only mock policy definitions against observed Invocation facts with ArkType. They explore deterministic authorization from a Workspace Owner-installed policy; ArkType is not proposed as Coral’s production authorization engine, and the stories do not model an agent approving another agent. A passing envelope applies only to the exact Invocation shown. Toggle \`showAuthorityEnvelopeMatch\` to run this experiment for a compatible dataset.

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

function createEnvelopeDataset({
  body,
  issueState,
  repository,
}: {
  body: string
  issueState?: string
  repository: string
}): OperationApprovalModel {
  const [org, repo] = repository.split('/')
  const invocationArguments: OperationApprovalModel['invocationArguments'] = [
    {
      label: 'Argument 1',
      value: { body, issue_number: 85, org, repo },
    },
    { label: 'Argument 2', value: { format: 'markdown' } },
    { label: 'Argument 3', value: 'notify-requester' },
  ]

  return {
    approvalAuthority: 'Workspace owner',
    authorityEnvelope: {
      envelopeId: 'env_01K4B6ZA',
      facts: {
        body,
        commentsToday: 7,
        evaluatedAt: '2026-09-02T12:00:00Z',
        issueState,
        operationCallPath: 'coral.providers.github.issues.createComment',
        repository,
      },
      installedBy: 'Workspace Owner',
      policy: {
        allowedOperationCallPath: 'coral.providers.github.issues.createComment',
        allowedRepository: 'withcoral/lagoon',
        dailyCommentLimit: 20,
        expiresAt: '2026-09-30T23:59:00Z',
        forbiddenMassMentions: ['@channel', '@here'],
        maxBodyCharacters: 2000,
        requiredIssueState: 'open',
      },
    },
    expiresAt: '2026-09-02 16:00 UTC',
    identity: 'coral-bot',
    invocationArguments,
    invokingPrincipal: 'triage-bot',
    operationCallPath: 'coral.providers.github.issues.createComment',
    policyText: 'Agents cannot approve their own operations.',
    provider: 'GitHub',
    providerReference:
      'Creates a comment on an issue in the selected repository using the authenticated GitHub account.',
    rawInvocation: invocationArguments.map(({ value }) => value),
    requestContext: {
      execIntent: 'Post the prepared triage note to the selected GitHub issue.',
      taskId: 'task_01K4B7TP',
      taskIntent: 'Triage the approval-policy follow-up for the Lagoon workspace.',
    },
    runContext: {
      runId: 'run_01K4B7V2',
      status: 'running',
      workspace: 'Lagoon',
    },
    technicalDetails: [
      { label: 'Operation invocation', value: 'inv_01K4B7X4 · pending' },
      { label: 'Provider generation', value: 'github@gen_0198' },
      { label: 'Credential route', value: 'route_github_coral_bot' },
    ],
  }
}

function evaluateAuthorityEnvelope(envelope: AuthorityEnvelope): AuthorityEnvelopeEvaluation {
  const { facts, policy } = envelope
  const operationPolicy = type('string').narrow(
    (value) => value === policy.allowedOperationCallPath,
  )
  const repositoryPolicy = type('string').narrow((value) => value === policy.allowedRepository)
  const bodyLengthPolicy = type('string').narrow(
    (value) => value.length <= policy.maxBodyCharacters,
  )
  const mentionPolicy = type('string').narrow((value) =>
    policy.forbiddenMassMentions.every((mention) => !value.includes(mention)),
  )
  const issueStatePolicy = type('string').narrow((value) => value === policy.requiredIssueState)
  const quotaPolicy = type('number').narrow((value) => value < policy.dailyCommentLimit)
  const expiryPolicy = type('Date').narrow(
    (evaluatedAt) => evaluatedAt.getTime() < new Date(policy.expiresAt).getTime(),
  )
  const detectedMassMentions = policy.forbiddenMassMentions.filter((mention) =>
    facts.body.includes(mention),
  )
  const checks: AuthorityEnvelopeEvaluation['checks'] = [
    {
      label: 'Operation',
      observed: facts.operationCallPath,
      policy: `Equals ${policy.allowedOperationCallPath}`,
      status: arkStatus(operationPolicy(facts.operationCallPath)),
    },
    {
      label: 'Repository',
      observed: facts.repository,
      policy: `Equals ${policy.allowedRepository}`,
      status: arkStatus(repositoryPolicy(facts.repository)),
    },
    {
      label: 'Issue state',
      observed: facts.issueState ?? 'Unavailable',
      policy: `Must be ${policy.requiredIssueState}`,
      status:
        facts.issueState === undefined ? 'unknown' : arkStatus(issueStatePolicy(facts.issueState)),
    },
    {
      label: 'Body length',
      observed: `${facts.body.length.toLocaleString()} characters`,
      policy: `At most ${policy.maxBodyCharacters.toLocaleString()} characters`,
      status: arkStatus(bodyLengthPolicy(facts.body)),
    },
    {
      label: 'Mention policy',
      observed:
        detectedMassMentions.length > 0 ? detectedMassMentions.join(', ') : 'No mass mentions',
      policy: `Excludes ${policy.forbiddenMassMentions.join(' and ')}`,
      status: arkStatus(mentionPolicy(facts.body)),
    },
    {
      label: 'Daily quota',
      observed:
        facts.commentsToday === undefined
          ? 'Unavailable'
          : `${facts.commentsToday} comments used today`,
      policy: `Fewer than ${policy.dailyCommentLimit} comments used today`,
      status:
        facts.commentsToday === undefined ? 'unknown' : arkStatus(quotaPolicy(facts.commentsToday)),
    },
    {
      label: 'Policy expiry',
      observed: `Evaluated ${facts.evaluatedAt}`,
      policy: `Expires ${policy.expiresAt}`,
      status: arkStatus(expiryPolicy(new Date(facts.evaluatedAt))),
    },
  ]

  return {
    checks,
    decision: checks.every(({ status }) => status === 'pass') ? 'allow' : 'requiresApproval',
    envelopeId: envelope.envelopeId,
    expiresAt: policy.expiresAt,
    installedBy: envelope.installedBy,
  }
}

function arkStatus(result: unknown): EnvelopeCheckStatus {
  return result instanceof type.errors ? 'fail' : 'pass'
}

const datasets: Record<DatasetKey, OperationApprovalModel> = {
  envelopeMatches: createEnvelopeDataset({
    body: 'Approval envelope exploration is ready for review.',
    issueState: 'open',
    repository: 'withcoral/lagoon',
  }),
  envelopeOutside: createEnvelopeDataset({
    body: '@channel Approval envelope exploration is ready for review.',
    repository: 'withcoral/private-roadmap',
  }),
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
    dataset: 'envelopeMatches',
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
    dataset: 'envelopeOutside',
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

function OperationApprovalStory({ dataset, ...props }: OperationApprovalStoryProps) {
  return <OperationApproval approval={datasets[dataset]} {...props} />
}

function OperationApproval({
  approval,
  argumentsCollapsible,
  argumentsInitiallyExpanded,
  compact,
  onApprove,
  onDecline,
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
  const hasDecisionContext = Boolean(showApprovalAuthority)
  const envelopeEvaluation =
    showAuthorityEnvelopeMatch && approval.authorityEnvelope
      ? evaluateAuthorityEnvelope(approval.authorityEnvelope)
      : undefined
  const hasContext = Boolean(
    showArguments ||
    showExpiry ||
    showAuthorityEnvelopeMatch ||
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
          initiallyExpanded={argumentsInitiallyExpanded}
          invocationArguments={approval.invocationArguments}
        />
      ) : null}

      {envelopeEvaluation ? <EnvelopeCheckSection evaluation={envelopeEvaluation} /> : null}

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
        <ApprovalActions onApprove={onApprove} onDecline={onDecline} />
      )}
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
      ) : hasContext ? (
        <Pill color="amber">Approval required</Pill>
      ) : null}
    </div>
  )
}

function InvocationArgumentsSection({
  collapsible,
  initiallyExpanded,
  invocationArguments,
}: {
  collapsible?: boolean
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
            <DetailRow
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
            {matches ? 'Matches envelope' : 'Outside envelope'}
          </Typography.BodySmallStrong>
          <Typography.BodySmall as="p" style={zeroMarginStyle} variant="secondary">
            {matches ? 'Can continue without per-call approval' : 'Requires approval'}
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
  const statusLabel = status === 'pass' ? 'Pass' : status === 'fail' ? 'Fails' : 'Unknown'
  const statusColor = status === 'pass' ? 'green' : status === 'fail' ? 'red' : 'amber'

  return (
    <div style={{ ...detailRowStyle, ...(!showDivider ? lastDetailRowStyle : {}) }}>
      <Typography.BodySmall as="dt" variant="tertiary">
        {label}
      </Typography.BodySmall>
      <div style={envelopeCheckValueStyle}>
        <div style={envelopeCheckCopyStyle}>
          <Typography.BodySmall as="dd" style={detailValueStyle} variant="primary">
            Policy: {policy}
          </Typography.BodySmall>
          <Typography.BodySmall as="p" style={zeroMarginStyle} variant="tertiary">
            Observed: {observed}
          </Typography.BodySmall>
        </div>
        <Pill color={statusColor}>{statusLabel}</Pill>
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
  onApprove,
  onDecline,
}: {
  onApprove: () => void
  onDecline: () => void
}) {
  return (
    <div style={actionsStyle}>
      <Button.Container onClick={onDecline} size="32" variant="secondary">
        <Button.Text>Decline</Button.Text>
      </Button.Container>
      <Button.Container onClick={onApprove} size="32" variant="primary">
        <Button.Text>Approve</Button.Text>
      </Button.Container>
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
