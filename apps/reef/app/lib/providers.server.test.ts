import { expect, it } from 'vitest'
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import {
  CredentialRoute_Kind,
  ProviderSchema,
  SearchIndexFailureReason,
  SearchIndexState,
} from '@/generated/coral/management/v1/provider_pb'
import type { PersonalAccountState } from '@/views/accounts/types'
import { personalAccountAction, providerFailure, searchIndexView } from './providers.server'

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
