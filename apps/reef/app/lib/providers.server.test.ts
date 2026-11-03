import { expect, it } from 'vitest'
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { ToolAccessSchema } from '@/generated/coral/management/v1/policy_pb'
import {
  CredentialRoute_Kind,
  ProviderSchema,
  SearchIndexFailureReason,
  SearchIndexState,
} from '@/generated/coral/management/v1/provider_pb'
import type { PersonalAccountState } from '@/views/accounts/types'
import { fakeManagement } from './management.test-support'
import { RECIPE_CHOICES } from './provider-recipes/library.server'
import { CORAL_ICONS } from '@/views/providers/coral-icons'
import {
  activityProviderMarks,
  personalAccountAction,
  providerLogos,
  providerMarks,
  providerFailure,
  searchIndexView,
  toolName,
  toolView,
} from './providers.server'

const name = 'workspaces/w/providers/p'

// Catches: a provider you cannot call yet reading as ready, or a provider with
// no account of yours asking you to connect one.
it.each([
  {
    setting: 'caller',
    route: CredentialRoute_Kind.CALLER,
    state: undefined,
    action: 'Connect your account',
  },
  {
    setting: 'caller',
    route: CredentialRoute_Kind.CALLER,
    state: 'not_connected',
    action: 'Connect your account',
  },
  {
    setting: 'caller',
    route: CredentialRoute_Kind.CALLER,
    state: 'needs_reconnect',
    action: 'Reconnect your account',
  },
  { setting: 'caller', route: CredentialRoute_Kind.CALLER, state: 'connected', action: null },
  { setting: 'fixed', route: CredentialRoute_Kind.FIXED, state: 'not_connected', action: null },
  { setting: 'none', route: CredentialRoute_Kind.NONE, state: undefined, action: null },
] as const)('asks a $setting provider with $state for $action', ({ route, state, action }) => {
  const provider = create(ProviderSchema, { name, credentialRoute: { kind: route } })
  const accounts = new Map<string, PersonalAccountState>(state ? [[name, state]] : [])
  expect(personalAccountAction(provider, accounts)).toBe(action)
})

// Catches: a reason that is missing from the map falling through to the
// form-fields sentence, which sends the reader to correct a form that is right.
it.each([
  [
    'OUTBOUND_DESTINATION_REJECTED',
    "Coral's outbound settings do not allow this address. Check the deployment's outbound settings before you try again.",
  ],
  [
    'UNMAPPED_PRECONDITION',
    'Coral could not read the document or server. Check the form fields. Correct any errors before you try again.',
  ],
])('maps %s to its own sentence or to the form-fields fallback', (reason, message) => {
  const result = providerFailure(
    new ConnectError(reason, Code.FailedPrecondition),
    'Coral could not read the document or server. ',
  )
  expect(result.init?.status).toBe(409)
  expect(result.data).toEqual({ error: message, credential: null })
})

// Catches: a provider that search indexed showing a notice, a pending
// provider showing none, or a failure showing no reason because its kind is
// missing from the map.
it.each([
  [SearchIndexState.INDEXED, SearchIndexFailureReason.UNSPECIFIED, null],
  [SearchIndexState.UNSPECIFIED, SearchIndexFailureReason.UNSPECIFIED, null],
  [SearchIndexState.PENDING, SearchIndexFailureReason.UNSPECIFIED, { state: 'pending' }],
  [
    SearchIndexState.FAILED,
    SearchIndexFailureReason.INVALID_ENTRY,
    { state: 'failed', reason: 'One of the tools cannot become a search entry.' },
  ],
  [
    SearchIndexState.FAILED,
    SearchIndexFailureReason.TIMED_OUT,
    { state: 'failed', reason: 'Indexing took too long.' },
  ],
  [
    SearchIndexState.FAILED,
    SearchIndexFailureReason.CLOCK,
    { state: 'failed', reason: 'The server clock is wrong.' },
  ],
  [
    SearchIndexState.FAILED,
    SearchIndexFailureReason.CORRUPT,
    { state: 'failed', reason: 'The search index is damaged.' },
  ],
  [
    SearchIndexState.FAILED,
    SearchIndexFailureReason.PROVIDER,
    { state: 'failed', reason: "Search cannot read the provider's tools." },
  ],
  [
    SearchIndexState.FAILED,
    SearchIndexFailureReason.DATABASE,
    { state: 'failed', reason: 'Search cannot use its database.' },
  ],
  [
    SearchIndexState.FAILED,
    SearchIndexFailureReason.UNSPECIFIED,
    { state: 'failed', reason: 'No reason recorded.' },
  ],
])('reads search index state %s with reason %s', (state, reason, view) => {
  const provider = create(ProviderSchema, {
    name,
    searchIndexState: state,
    searchIndexFailureReason: reason,
  })
  expect(searchIndexView(provider)).toEqual(view)
})

// Catches: Reef encoding a Tool Key differently from Coral, which rejects any
// name that is not unpadded URL-safe base64 only when a policy is saved.
it.each([
  ['admin.DeleteEdge', 'YWRtaW4uRGVsZXRlRWRnZQ'],
  ['~~~', 'fn5-'],
  ['???', 'Pz8_'],
  ['café', 'Y2Fmw6k'],
])('keeps the Tool Key %s through its resource name', (key, encoded) => {
  const tool = toolName(name, key)
  expect(tool).toBe(`${name}/tools/${encoded}`)
  expect(toolView(create(ToolAccessSchema, { tool })).key).toBe(key)
})

const withIcons = RECIPE_CHOICES.find((choice) => choice.icons)!
const marksOf = (listProviders: () => object) =>
  providerMarks(
    fakeManagement({ providers: { listProviders } }),
    'workspaces/w',
    AbortSignal.any([]),
  )

// A provider usually keeps the id of the recipe it came from, so that recipe's logo stands for
// it. Coral's mark stands for the Built-in Provider.
it('marks each provider with the logo that stands for it', async () => {
  const marks = await marksOf(() => ({
    providers: [
      {
        callRoot: 'coral.workspace',
        displayName: 'Workspace',
        builtin: true,
        namespace: 'workspace',
      },
      { callRoot: `coral.providers.${withIcons.id}`, displayName: 'Ours', namespace: withIcons.id },
      { callRoot: 'coral.providers.handmade', displayName: 'Handmade', namespace: 'handmade' },
    ],
  }))
  expect(marks).toEqual([
    { callRoot: 'coral.workspace', displayName: 'Workspace', icons: CORAL_ICONS },
    { callRoot: `coral.providers.${withIcons.id}`, displayName: 'Ours', icons: withIcons.icons },
    { callRoot: 'coral.providers.handmade', displayName: 'Handmade' },
  ])
})

// The marks only decorate rows that name their call, so an outage leaves the rows as they are.
it('marks no provider while Coral cannot list them', async () => {
  expect(
    await marksOf(() => {
      throw new ConnectError('down', Code.Unavailable)
    }),
  ).toEqual([])
})

// Catches: Activity failing its page because the logos could not load, or a later page of
// a loaded list listing every provider again.
it.each([
  [
    'a page',
    '/activity',
    () => ({ providers: [{ callRoot: 'coral.providers.p', displayName: 'P' }] }),
    [{ callRoot: 'coral.providers.p', displayName: 'P' }],
    1,
  ],
  [
    'a refused read',
    '/activity',
    () => {
      throw new ConnectError('no', Code.PermissionDenied)
    },
    [],
    1,
  ],
  [
    'a later page of a loaded list',
    '/activity?load=page&after=x',
    () => ({ providers: [] }),
    [],
    0,
  ],
])('names Activity calls for %s', async (_, path, listProviders, marks, reads) => {
  let read = 0
  const api = fakeManagement({
    providers: {
      listProviders: () => {
        read++
        return listProviders()
      },
    },
  })
  expect(
    await activityProviderMarks(api, 'workspaces/w', new Request(`http://reef${path}`)),
  ).toEqual(marks)
  expect(read).toBe(reads)
})

// Catches: a decorative read that hides a lapsed sign-in, so the page never asks you to sign in.
it('fails Activity for a lapsed sign-in', async () => {
  const api = fakeManagement({
    providers: {
      listProviders: () => {
        throw new ConnectError('expired', Code.Unauthenticated)
      },
    },
  })
  await expect(
    activityProviderMarks(api, 'workspaces/w', new Request('http://reef/activity')),
  ).rejects.toMatchObject({ code: Code.Unauthenticated })
})

it('keys each logo by its provider, and leaves out a provider without one', async () => {
  const logos = await providerLogos(
    fakeManagement({
      providers: {
        listProviders: () => ({
          providers: [
            { name: 'workspaces/w/providers/ours', namespace: withIcons.id },
            { name: 'workspaces/w/providers/handmade', namespace: 'handmade' },
          ],
        }),
      },
    }),
    'workspaces/w',
  )
  expect([...logos]).toEqual([['workspaces/w/providers/ours', withIcons.icons]])
})
