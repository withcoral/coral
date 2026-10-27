/** @vitest-environment happy-dom */

import { act, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useEffect, useState } from 'react'
import { describe, expect, it } from 'vitest'

import { renderRoute } from '@/test-support/route'
import { approvalContext, approvalRequest as approval } from '../activity/fixtures'
import { ActivityArchivedContext } from '../activity/live'
import { ActionReviewSession, ApprovalReview } from './review'

type Approval = typeof approval
type DecisionResponse = { error: string } | { state: string } | { recording: true }

const approved: DecisionResponse = { state: 'approved' }
// The instructions an invalid comment describes itself with.
const correction = 'Use at most 1024 UTF-8 bytes. Remove null characters.'

function renderReview() {
  const submissions: Array<{
    form: FormData
    respond: (response: DecisionResponse) => void
  }> = []
  let updateRequest!: (request: Approval | null) => void
  function ReviewPage() {
    const [request, setRequest] = useState<Approval | null>(approval)
    useEffect(() => {
      updateRequest = setRequest
    }, [])
    return (
      <ActionReviewSession request={request}>
        {request ? (
          <ApprovalReview detail={request} context={approvalContext} />
        ) : (
          <p>Reconnecting to the exact review.</p>
        )}
      </ActionReviewSession>
    )
  }
  const { container, router } = renderRoute(<ReviewPage />, {
    entry: '/activity?record=one&request=one',
    // The shell owns the CSRF token that review forms submit.
    csrf: 'test-csrf',
    routes: [
      {
        path: '/activity/resolve',
        action: async ({ request }) => {
          const form = await request.formData()
          return new Promise<DecisionResponse>((respond) => submissions.push({ form, respond }))
        },
      },
      { path: '/elsewhere', element: <p>Another page</p> },
    ],
  })
  return {
    container,
    submissions,
    location: () => router.state.location,
    navigate: async (to: string, options?: Parameters<typeof router.navigate>[1]) => {
      await act(async () => {
        await router.navigate(to, options)
      })
    },
    show: async (request: Approval | null) => {
      await act(async () => updateRequest(request))
    },
  }
}

const user = userEvent.setup()
const comment = () => screen.getByRole('textbox', { name: 'Comment (optional)' })
const button = (name: string) => screen.getByRole('button', { name })
const statuses = () => screen.getAllByRole('status').map((status) => status.textContent)

async function enterComment(value: string) {
  await user.clear(comment())
  await user.paste(value)
}

async function click(name: string) {
  await user.click(button(name))
}

describe('exact approval review', () => {
  // Catches: arguments re-serialized to set an inline document aside, which rounds a large
  // integer, drops a duplicate key, and shows a placeholder as if the call had sent it. The
  // text as sent is the one source: no second copy of the document.
  it.each([
    [
      'a JSON object',
      '{"namespace":"weather","source":{"kind":"openapi_document","document":"openapi: 3.1.0"},"credentialRoute":"none"}',
      [
        ['namespace', '"weather"'],
        ['source', '"document": "openapi: 3.1.0"'],
        ['credentialRoute', '"none"'],
      ],
    ],
    [
      'an integer beyond double precision and a duplicate key',
      '{"namespace":"weather","source":{"kind":"openapi_document","document":"openapi: 3.1.0"},"limit":9007199254740993,"note":"first","note":"second"}',
      [],
    ],
  ])('shows %s with an inline OpenAPI document as the call sent it', (_, sent, fields) => {
    const detail = { ...approval, callPath: 'coral.workspace.addProvider', arguments: sent }
    renderRoute(
      <ActionReviewSession request={detail}>
        <ApprovalReview detail={detail} context={approvalContext} />
      </ActionReviewSession>,
      { entry: '/activity', csrf: 'test-csrf' },
    )
    const shown = screen.getByText(/^Arguments/, { selector: 'summary' }).closest('details')!
    expect(shown).not.toHaveTextContent('shown below')
    if (fields.length) {
      for (const [key, value] of fields)
        expect(
          within(shown).getByText(key, { selector: 'dt' }).nextElementSibling,
        ).toHaveTextContent(value)
    } else {
      // JSON that rows would change stays one block, exactly as sent.
      expect(shown.querySelector('dl')).toBeNull()
      const block = shown.querySelector('pre')!.textContent!
      expect(block).toContain('9007199254740993')
      expect(block).toContain('"first"')
      expect(block).toContain('"second"')
    }
    expect(screen.queryByText('OpenAPI document', { selector: 'summary' })).toBeNull()
  })

  it.each([
    [
      'approval to its exact operation',
      `/activity?record=${encodeURIComponent(approval.run)}&request=${encodeURIComponent(approval.name)}`,
      `/activity?record=${encodeURIComponent(approval.invocation)}&request=${encodeURIComponent(approval.name)}`,
      'record',
      approval.invocation,
    ],
    [
      'initial request',
      '/activity?record=one',
      `/activity?record=one&request=${encodeURIComponent(approval.name)}`,
      'request',
      approval.name,
    ],
  ])(
    'pins the already displayed %s without discarding a draft',
    async (_label, shown, pinned, key, value) => {
      const page = renderReview()
      await page.navigate(shown)
      await enterComment('Keep this draft')
      await page.navigate(pinned, { replace: true, defaultShouldRevalidate: false })
      expect(new URLSearchParams(page.location().search).get(key)).toBe(value)
      expect(comment()).toHaveValue('Keep this draft')
      expect(document.body).not.toHaveTextContent('Discard your comment?')
    },
  )
  it('rejects 1025 UTF-8 bytes but submits exactly 1024 bytes of multibyte comment', async () => {
    const page = renderReview()
    // Each é is two UTF-8 bytes, so the boundary is not a character limit.
    const atLimit = 'é'.repeat(512)
    await enterComment(`${atLimit}a`)

    expect(button('Approve this call')).toBeDisabled()
    expect(button('Deny this call')).toBeDisabled()
    expect(comment()).toBeInvalid()
    expect(comment()).toHaveAccessibleDescription(correction)
    await click('Approve this call')
    await click('Deny this call')
    expect(page.submissions).toHaveLength(0)

    await enterComment(atLimit)
    expect(comment()).toBeValid()
    expect(button('Approve this call')).toBeEnabled()
    expect(button('Deny this call')).toBeEnabled()
    await click('Approve this call')
    expect(page.submissions).toHaveLength(1)
    expect(Object.fromEntries(page.submissions[0].form)).toEqual({
      csrf: 'test-csrf',
      name: approval.name,
      intent: 'approve',
      comment: atLimit,
    })
    await act(async () => page.submissions[0].respond(approved))
  })

  it('rejects null characters in a short comment and permits correction', async () => {
    const page = renderReview()
    await enterComment('Review\0note')

    expect(button('Approve this call')).toBeDisabled()
    expect(button('Deny this call')).toBeDisabled()
    expect(comment()).toBeInvalid()
    expect(comment()).toHaveAccessibleDescription(correction)
    await click('Approve this call')
    await click('Deny this call')
    expect(page.submissions).toHaveLength(0)

    await enterComment('Review note')
    expect(button('Approve this call')).toBeEnabled()
    expect(button('Deny this call')).toBeEnabled()
    expect(comment()).toBeValid()
    expect(page.container).not.toHaveTextContent('Remove null characters.')
    await click('Deny this call')
    expect(page.submissions).toHaveLength(1)
    expect(page.submissions[0].form.get('comment')).toBe('Review note')
    expect(page.submissions[0].form.get('intent')).toBe('decline')
    await act(async () => page.submissions[0].respond({ state: 'declined' }))
  })

  it.each([
    ['Approve this call', 'Deny this call', 'approve'],
    ['Deny this call', 'Approve this call', 'decline'],
  ])(
    'prevents duplicate or opposite decisions while %s is pending',
    async (label, other, intent) => {
      const page = renderReview()
      await enterComment('Reviewed the exact arguments.')
      await click(label)

      expect(page.submissions).toHaveLength(1)
      expect(page.submissions[0].form.get('intent')).toBe(intent)
      expect(button(label)).toHaveAttribute('aria-busy', 'true')
      expect(button(label)).toHaveAttribute('aria-disabled', 'true')
      expect(button(other)).toBeDisabled()
      await click(label)
      await click(other)
      expect(page.submissions).toHaveLength(1)
      expect(comment()).toHaveValue('Reviewed the exact arguments.')

      await act(async () => page.submissions[0].respond({ error: 'Try again.' }))
      expect(button(label)).toBeEnabled()
      expect(button(other)).toBeEnabled()
    },
  )

  it('retains the comment after an action error and submits the corrected comment on retry', async () => {
    const page = renderReview()
    await enterComment('Original review note')
    await click('Approve this call')
    expect(page.submissions).toHaveLength(1)
    await act(async () =>
      page.submissions[0].respond({ error: 'Could not save your decision. Try again.' }),
    )

    expect(page.container).toHaveTextContent('Could not save your decision. Try again.')
    expect(comment()).toHaveValue('Original review note')
    expect(button('Approve this call')).toBeEnabled()
    expect(button('Deny this call')).toBeEnabled()

    await enterComment('Corrected review note')
    await click('Approve this call')
    expect(page.submissions).toHaveLength(2)
    expect(Object.fromEntries(page.submissions[1].form)).toEqual({
      csrf: 'test-csrf',
      name: approval.name,
      intent: 'approve',
      comment: 'Corrected review note',
    })
    await act(async () => page.submissions[1].respond(approved))

    // The header states the outcome; no banner repeats it.
    expect(statuses()).toEqual(['Approved'])
    expect(page.container).not.toHaveTextContent('Could not save your decision.')
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument()
  })

  // Catches: a decision that Coral holds offered again, or its status outliving the live read.
  it('says your decision is saving until the live read shows it decided', async () => {
    const page = renderReview()
    await enterComment('Reviewed the exact arguments.')
    await click('Approve this call')
    await act(async () => page.submissions[0].respond({ recording: true }))

    expect(statuses()).toContain('Saving your decision…')
    expect(button('Approve this call')).toBeDisabled()
    expect(button('Deny this call')).toBeDisabled()
    // The comment went with the decision, so it is no longer an unsent draft.
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument()

    await page.show({ ...approval, state: 'approved', actionable: false })
    expect(statuses()).toEqual(['Approved'])
  })

  it('shows the durable denial returned to a stale approval attempt and removes decision controls', async () => {
    const page = renderReview()
    await enterComment('I intended to approve this call.')
    await click('Approve this call')
    expect(page.submissions).toHaveLength(1)
    expect(page.submissions[0].form.get('intent')).toBe('approve')
    await act(async () => page.submissions[0].respond({ state: 'declined' }))

    // An unchanged pending read must not overwrite the winning action response.
    await page.show({ ...approval })
    expect(statuses()).toEqual(['Denied'])
    expect(page.container).not.toHaveTextContent('Approval needed')
    expect(page.container).not.toHaveTextContent('Approved')
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /this call/ })).not.toBeInTheDocument()
  })

  it('does not guard a submitted comment as unsent after a durable winning denial', async () => {
    const page = renderReview()
    await enterComment('I intended to approve this call.')
    await click('Approve this call')
    expect(page.submissions).toHaveLength(1)
    await act(async () => page.submissions[0].respond({ state: 'declined' }))
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument()

    await page.navigate('/elsewhere')
    expect(page.container.textContent).toBe('Another page')
    expect(document.body).not.toHaveTextContent('Discard your comment?')
  })

  it('keeps a concealed draft guarded and restores it when the exact review returns', async () => {
    const page = renderReview()
    await enterComment('Keep this unsent review note.')
    await page.show(null)
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument()
    expect(page.container).not.toHaveTextContent('Keep this unsent review note.')

    await page.navigate('/elsewhere')
    expect(page.location().pathname).toBe('/activity')
    expect(document.body).toHaveTextContent('Discard your comment?')
    await click('Keep editing')
    await page.show({ ...approval })
    expect(comment()).toHaveValue('Keep this unsent review note.')
    expect(page.submissions).toHaveLength(0)

    await page.navigate('/elsewhere')
    await click('Discard comment')
    expect(page.container.textContent).toBe('Another page')
  })
})

/** The disclosure whose summary reads `title`. */
function disclosure(title: string) {
  const summary = screen.getByText(title, { selector: 'summary' })
  return summary.closest('details')!
}
/** The review's first list: why the call runs. */
function contextList() {
  return screen.getByText('Requested by', { selector: 'dt' }).closest('dl') as HTMLElement
}
/** Label and value of each row in `scope`, in order. */
function rows(scope: HTMLElement) {
  const terms = within(scope).queryAllByRole('term')
  const values = within(scope).queryAllByRole('definition')
  return terms.map((term, index) => [term.textContent, values[index].textContent])
}

function renderStatic(detail: Approval) {
  return renderRoute(
    <ActionReviewSession request={detail}>
      <ApprovalReview detail={detail} context={approvalContext} />
    </ActionReviewSession>,
    { entry: '/activity', csrf: 'test-csrf' },
  )
}

describe('approval review layout', () => {
  it('orients the reviewer before the exact arguments and keeps supporting detail collapsed', () => {
    renderStatic({ ...approval, providerLabel: 'Mail', taskIntent: 'Chase overdue invoices' })
    const review = screen.getByRole('region', { name: `Review ${approval.callPath}` })
    expect(within(review).getByRole('heading', { name: approval.callPath })).toBeInTheDocument()
    expect(within(review).getByText('approval request')).toBeInTheDocument()
    expect(review).toHaveTextContent('Provider: Mail')
    expect(review).toHaveTextContent('Approval required')
    // Why the call runs, in one list without a heading.
    expect(screen.queryByRole('heading', { name: 'Context' })).not.toBeInTheDocument()
    expect(rows(contextList())).toEqual([
      ['Task intent', 'Chase overdue invoices'],
      ['Run intent', 'Send invoice reminders'],
      ['Requested by', 'Collections agent · Finance'],
    ])
    expect(disclosure('Arguments (1)')).toHaveAttribute('open')
    // No program body: the review decides this one call, not the program.
    expect(review).not.toHaveTextContent('Program body')
    expect(review).not.toHaveTextContent(approvalContext.source)
    expect(disclosure('Add a comment')).not.toHaveAttribute('open')
    // No technical details: the workspace, provider and request names are not for reading.
    expect(review).not.toHaveTextContent('Technical details')
    for (const id of [approvalContext.run.workspace, approval.provider, approval.name])
      expect(review).not.toHaveTextContent(id)
    expect(review).not.toHaveTextContent('Approval applies only to this call')
    // The order a reviewer reads: why, what exactly, then how to decide.
    const order = ['Task intent', 'Arguments (1)', 'Add a comment']
    const positions = order.map((text) => review.textContent!.indexOf(text))
    expect(positions).toEqual(positions.toSorted((a, b) => a - b))
    expect(review.textContent!.indexOf('Add a comment')).toBeLessThan(
      review.textContent!.indexOf('Approve this call'),
    )
  })

  // Catches: a review dated by its Run or its deadline instead of its call.
  it('states when its call started, after who requested it', () => {
    renderRoute(
      <ActionReviewSession request={approval}>
        <ApprovalReview
          detail={approval}
          context={approvalContext}
          callStartedAt="2026-09-22T09:00:05Z"
        />
      </ActionReviewSession>,
      { entry: '/activity', csrf: 'test-csrf' },
    )
    const list = contextList()
    expect(
      rows(list)
        .map(([term]) => term)
        .slice(-2),
    ).toEqual(['Requested by', 'Call started'])
    expect(rows(list).at(-1)![1]).toBe('22 Sep 2026, 09:00:05 UTC')
    expect(list.querySelector('time')).toHaveAttribute('dateTime', '2026-09-22T09:00:05Z')
  })

  // Catches: placeholders such as "Agent unavailable" or "Provider: unavailable" for labels Coral
  // did not send, or a review that loses who asked for the call.
  it.each([
    ['an agent without a name', { actorLabel: undefined }, 'Unknown agent · Finance'],
    ['a workspace without a name', { workspaceLabel: undefined }, 'Collections agent'],
    [
      'your own run without names',
      { actorLabel: undefined, agent: '', workspaceLabel: undefined },
      'You',
    ],
  ])('names who asked with %s, and no placeholder', (_, labels, requested) => {
    renderRoute(
      <ActionReviewSession request={approval}>
        <ApprovalReview
          detail={{ ...approval, providerLabel: undefined }}
          context={{ ...approvalContext, run: { ...approvalContext.run, ...labels } }}
        />
      </ActionReviewSession>,
      { entry: '/activity', csrf: 'test-csrf' },
    )
    const review = screen.getByRole('region', { name: `Review ${approval.callPath}` })
    expect(rows(contextList()).at(-1)).toEqual(['Requested by', requested])
    expect(review.textContent).not.toMatch(/unavailable|Provider:/)
    expect(review).not.toHaveTextContent(approvalContext.run.agent)
  })

  // Catches: a decided review that drops the decision because the reviewer's name is missing.
  it('keeps a decision whose reviewer has no name', () => {
    renderStatic({
      ...approval,
      state: 'approved',
      actionable: false,
      approver: 'users/gone',
      approverLabel: undefined,
      decidedAt: '2026-09-22T09:01:00Z',
      comment: null,
    })
    const decision = screen.getByText('Reviewed by', { selector: 'dt' }).closest('dl')!
    expect(rows(decision as HTMLElement)[0]).toEqual(['Reviewed by', 'Previous reviewer'])
  })

  it('omits context it does not have instead of inventing it', () => {
    renderStatic({ ...approval, taskIntent: undefined })
    expect(rows(contextList())).toEqual([
      ['Run intent', 'Send invoice reminders'],
      ['Requested by', 'Collections agent · Finance'],
    ])
  })

  it.each([
    [
      'an object as one row per field',
      '{"to":"owner@example.com","cc":["a@example.com"],"limit":2,"draft":false}',
      'Arguments (4)',
      [
        ['to', '"owner@example.com"'],
        ['cc', '["a@example.com"]'],
        ['limit', '2'],
        ['draft', 'false'],
      ],
    ],
    [
      'strings apart from the values they spell',
      '{"limit":"2","note":""}',
      'Arguments (2)',
      [
        ['limit', '"2"'],
        ['note', '""'],
      ],
    ],
    ['an empty object without rows', '{}', 'Arguments (0)', []],
    ['an array as exact JSON', '["owner@example.com",2]', 'Arguments', []],
    ['a scalar as exact JSON', '"owner@example.com"', 'Arguments', []],
    ['invalid JSON unchanged', '{"to":', 'Arguments', []],
    // Parsing would round the number or drop the first key, so the rows would not be exact.
    ['an imprecise number as exact JSON', '{"id":12345678901234567890}', 'Arguments', []],
    ['a duplicate key as exact JSON', '{"to":"a","to":"b"}', 'Arguments', []],
  ])('shows %s', (_label, value, title, expected) => {
    renderStatic({ ...approval, arguments: value })
    const section = disclosure(title)
    expect(section).toHaveAttribute('open')
    expect(rows(section)).toEqual(expected)
    // The fallback formats whitespace only, so the original tokens remain.
    if (value === '{}') expect(section).toHaveTextContent('This call has no arguments.')
    else if (!expected.length)
      expect(section.textContent!.replace(/\s/g, '')).toContain(value.replace(/\s/g, ''))
  })

  it.each([
    ['approved', true, 'Approved'],
    ['declined', true, 'Denied'],
    ['expired', false, 'Expired'],
    ['pending', false, 'Approval unavailable'],
  ])('settles a %s review without decision controls', (state, decided, label) => {
    renderStatic({
      ...approval,
      state,
      actionable: false,
      ...(decided && {
        approver: 'owner',
        approverLabel: 'You',
        decidedAt: '2026-09-22T09:01:00Z',
        comment: 'Checked the recipient.',
      }),
    })
    expect(statuses()).toEqual([label])
    expect(screen.queryByText('Approval required')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /this call/ })).not.toBeInTheDocument()
    expect(screen.queryByText('Add a comment')).not.toBeInTheDocument()
    expect(screen.queryByText('Program body')).not.toBeInTheDocument()
    // The decision reads in a list of its own, without a heading.
    expect(screen.queryByRole('heading', { name: 'Decision' })).not.toBeInTheDocument()
    const decision = screen.queryByText('Reviewed by', { selector: 'dt' })?.closest('dl')
    if (decided) {
      expect(rows(decision as HTMLElement)).toEqual([
        ['Reviewed by', 'You'],
        ['Decided', '22 Sep 2026, 09:01:00 UTC'],
        ['Comment', 'Checked the recipient.'],
      ])
      expect(decision!.querySelector('time')).toHaveAttribute('dateTime', '2026-09-22T09:01:00Z')
    } else expect(decision).toBeFalsy()
  })
})

// Catches: an archived workspace offering a decision that Coral refuses.
it('an archived workspace shows the arguments but tells you to restore it before you decide', () => {
  renderRoute(
    <ActivityArchivedContext value="member">
      <ActionReviewSession request={approval}>
        <ApprovalReview detail={approval} context={approvalContext} />
      </ActionReviewSession>
    </ActivityArchivedContext>,
    { entry: '/activity?record=one&request=one', csrf: 'test-csrf' },
  )
  expect(screen.getByRole('heading', { name: approval.callPath })).toBeInTheDocument()
  expect(disclosure('Arguments (1)')).toHaveAttribute('open')
  expect(
    screen.getByText('Ask a workspace owner to restore this workspace. Then decide.'),
  ).toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /this call$/ })).toBeNull()
  expect(screen.queryByText('Add a comment')).toBeNull()
})
