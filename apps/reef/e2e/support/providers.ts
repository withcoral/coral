// Shared moves through the Provider pages. Every helper drives the real UI or
// the real MCP endpoint; none reaches into Reef or Coral internals.
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { expect } from 'vitest'
import type { Locator, Page, Request } from 'playwright'

import {
  CredentialRoute_Kind,
  type ProviderConfiguration,
  type ProviderSetup,
} from '../generated/coral/management/v1/provider_pb.ts'
import type { Target } from './config.ts'
import { managementClients } from './management.ts'
import { mcpToken } from './actors.ts'

export { collectDataResponses } from './data-responses.ts'

/** Creates a Workspace over the management API and returns its id. */
export async function createWorkspace(accessToken: string | undefined, name: string) {
  const workspace = await managementClients(accessToken).workspaces.createWorkspace({
    displayName: name,
    requestId: crypto.randomUUID(),
  })
  return workspace.name.split('/')[1]
}

/**
 * Creates a Workspace through the sidebar dialog and returns its id. Use it
 * only for a target the management clients cannot reach.
 */
export async function createWorkspaceViaUi(page: Page, name: string) {
  await page.goto('/')
  // Hydration first: a click that lands before it opens nothing.
  await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
  await page
    .getByRole('navigation', { name: 'Coral', exact: true })
    .getByRole('button', { name: 'Create workspace', exact: true })
    .click()
  await page.getByLabel('Workspace name', { exact: true }).fill(name)
  await page
    .getByRole('dialog')
    .getByRole('button', { name: 'Create workspace', exact: true })
    .click()
  await page.waitForURL((url) => /^\/workspaces\/[0-9a-f-]{36}$/.test(url.pathname))
  return new URL(page.url()).pathname.split('/')[2]
}

/** The breadcrumb trail, or the crumb that reads exactly `text`. */
export function breadcrumb(page: Page, text?: string) {
  const trail = page.getByRole('navigation', { name: 'breadcrumb', exact: true })
  return text === undefined ? trail : trail.getByText(text, { exact: true })
}

/**
 * Shows the custom source cards: an MCP server, then an OpenAPI document. Add
 * provider opens on the catalog, with the cards one tab away; Update provider
 * has no catalog, so the cards show at once.
 */
export async function openCustomSource(page: Page) {
  const tab = page.getByRole('tab', { name: 'Add custom', exact: true })
  const field = page.getByLabel('MCP server URL', { exact: true })
  await tab.or(field).first().waitFor()
  if (await tab.isVisible()) await tab.click()
  await field.waitFor()
}

/**
 * Chooses the OpenAPI document to read. The browser takes the file by path: a
 * buffer crosses the browser protocol as text, and a document near the 64 MiB
 * limit then outlasts the action timeout on a busy runner.
 */
export async function chooseFile(page: Page, name: string, buffer: Buffer) {
  const replace = page.getByRole('link', { name: 'Replace document', exact: true })
  const input = page.getByLabel('OpenAPI file', { exact: true })
  await input.or(replace).first().waitFor({ state: 'attached' })
  if (await replace.count()) await replace.click()
  await openCustomSource(page)
  const directory = await mkdtemp(join(tmpdir(), 'reel-upload-'))
  // The browser reads the file when the form submits, so it lasts as long as the page.
  page.once('close', () => void rm(directory, { recursive: true, force: true }))
  await writeFile(join(directory, name), buffer)
  await input.setInputFiles(join(directory, name))
}

/** Configuration moves on with a Next button; every later step uses a Next link. */
export function configurationNext(page: Page) {
  return page.getByRole('button', { name: 'Next', exact: true })
}

/**
 * Walks back to Configuration from wherever the attempt is. Every step's own
 * way back goes exactly one step, so this presses it until the configuration
 * form appears; a frozen attempt revises its choices first.
 */
export async function backToConfiguration(page: Page) {
  const configuring = configurationNext(page)
  const back = page.getByRole('link', { name: /^(Back|Change choices)$/ }).first()
  for (let hop = 0; hop < 4; hop += 1) {
    // A source fetch can still be settling when this helper starts. Wait for
    // either destination state before deciding to navigate away from it.
    await configuring.or(back).first().waitFor({ state: 'attached' })
    if (await configuring.count()) break
    const from = page.url()
    await back.click()
    await page.waitForURL((url) => url.href !== from)
  }
  await configuring.waitFor()
}

/**
 * Names the provider in Configuration and accepts the relisted operations.
 * Use this for drafts without a retained grant. Retention journeys name the
 * provider before authorization because reopening Configuration clears that choice.
 */
export async function setNamespace(page: Page, namespace: string) {
  if (!(await configurationNext(page).count())) await backToConfiguration(page)
  await page.getByLabel('Namespace', { exact: true }).fill(namespace)
  await confirmConfiguration(page)
  await acceptOperations(page)
}

/** One destination's URL field, named by the declaration it serves. */
export function destinationField(page: Page, label: string) {
  return page.getByLabel(label, { exact: true })
}

/**
 * Add provider from an uploaded document in a Workspace, through every step,
 * and land on the new Provider. Returns its URL. A document
 * that needs an account answers "Whose account does it use?" before any
 * sign-in; only a shared account enters a secret.
 */
export async function addProviderFromFile(
  page: Page,
  workspace: string,
  document: Buffer,
  namespace: string,
  options: {
    account?: AccountOutcome
    secret?: string
    authentication?: 'No authentication' | 'Bearer token' | 'OAuth authorization code'
  } = {},
) {
  await page.goto(`/workspaces/${workspace}/providers/new`)
  await page.getByRole('heading', { name: 'Add provider', exact: true, level: 1 }).waitFor()
  await chooseFile(page, `${namespace}.json`, document)
  await page.getByRole('button', { name: 'Read', exact: true }).click()
  await page.getByLabel('Display name', { exact: true }).waitFor()
  await page.getByLabel('Namespace', { exact: true }).fill(namespace)
  await confirmConfiguration(page, options.authentication)
  await acceptOperations(page)
  if (options.account) {
    await chooseAccount(page, options.account)
    if (options.secret !== undefined) await connectSharedAccount(page, options.secret)
  }
  await page.getByRole('button', { name: 'Add provider', exact: true }).waitFor()
  expect(new URL(page.url()).searchParams.get('namespace')).toBe(namespace)
  await page.getByRole('button', { name: 'Add provider', exact: true }).click()
  await waitForPublication(page)
  return new URL(page.url()).pathname
}

/**
 * Matches the data response of a publication that Coral committed: Add
 * redirects to the provider, an update to its settings. A refusal answers with
 * data instead.
 */
export const committedPublication =
  /"redirect","\/workspaces\/[^"]+\/providers\/[^"/]+(?:\/settings)?"/

/**
 * Saves a settings change that asks for confirmation first, then waits for the
 * provider's settings, where the change opened from.
 */
export async function saveConfirmedSettingsChange(page: Page) {
  await page.getByRole('button', { name: 'Save', exact: true }).click()
  await page
    .getByRole('dialog', { name: 'Save changes?' })
    .getByRole('button', { name: 'Save', exact: true })
    .click()
  await page.waitForURL((url) => url.pathname.endsWith('/settings'))
}

/**
 * Cancels a settings change that has unsaved changes: Cancel asks first, and
 * Discard changes returns to the provider's settings.
 */
export async function discardSettingsChange(page: Page) {
  await page.getByRole('button', { name: 'Cancel', exact: true }).click()
  await page
    .getByRole('dialog', { name: 'Discard your changes?' })
    .getByRole('button', { name: 'Discard changes', exact: true })
    .click()
  await page.waitForURL((url) => url.pathname.endsWith('/settings'))
}

/**
 * Waits for Add to land on the provider's own page. A callback or error page
 * outside the stepper does not count as a publication. Updates land on settings.
 */
export async function waitForPublication(page: Page) {
  await page.waitForURL((url) =>
    /^\/workspaces\/[^/]+\/providers\/(?!new$)[^/]+$/.test(url.pathname),
  )
}

/**
 * Runs `act` and waits for the redirect that finishes it to land on a page
 * with `flag` in the query. That page says so once in a success toast, then
 * drops the flag so a reload or Back does not repeat it.
 */
export async function expectArrivalToast(
  page: Page,
  { flag, title }: { flag: string; title: string },
  act: () => Promise<unknown>,
) {
  const arrived = page.waitForURL((url) => url.searchParams.has(flag))
  await act()
  await arrived
  await page.waitForURL((url) => !url.searchParams.has(flag))
  await page.getByText(title, { exact: true }).first().waitFor()
}

const postedForm = (request: Request) => new URLSearchParams(request.postData() ?? '')

/**
 * The form of the next Add provider submission. A sign-in started from the
 * account question adds the provider by itself once it lands, and publication
 * consumes the setup's grant, so a scenario reads the grant from the request.
 * Start waiting before the action that submits.
 */
export async function nextPublication(page: Page) {
  return postedForm(
    await page.waitForRequest(
      (request) => request.method() === 'POST' && postedForm(request).get('intent') === 'publish',
    ),
  )
}

/**
 * The providers the workspace added, without the Built-in Provider that Coral
 * adds to every workspace itself.
 */
export function addedProviders<P extends { builtin: boolean }>(providers: readonly P[]) {
  return providers.filter((provider) => !provider.builtin)
}

/** The rail of provider tabs beside the operations table. */
const providersRail = (page: Page) =>
  page.getByRole('navigation', { name: 'installed providers', exact: true })

/**
 * Opens the settings page of the provider the URL names, from its entry in
 * the rail. When the URL names no provider or the Built-in Provider, opens the
 * only added provider's settings instead.
 */
export async function openProviderSettings(page: Page) {
  // A saved change returns to settings. Load them again for fresh data.
  if (/\/providers\/[^/?#]+\/settings$/.test(new URL(page.url()).pathname)) {
    await page.reload()
    await breadcrumb(page, 'Settings').waitFor()
    return
  }
  // The gear sits in the rail beside its provider. Publication lands on the
  // provider route; the gear must belong to that document, not to the page a
  // redirect is about to replace.
  await page.waitForURL((url) => /\/providers(?:\/(?!new(?:\/|$))[^/?#]+)?$/.test(url.pathname))
  const routed = /\/providers\/([^/?#]+)/.exec(new URL(page.url()).pathname)?.[1]
  // The bare list route is the All providers tab and names no provider. Each
  // added provider's gear links to its settings route, beside the provider in
  // the rail; Coral has no gear. Wait for the
  // rail's gears before looking for the routed one, so a rail that has not
  // rendered yet cannot redirect the click.
  const rail = providersRail(page)
  const gears = rail.getByRole('link', { name: / settings$/ })
  await gears.first().waitFor()
  const routedGear = routed ? rail.locator(`a[href$="/providers/${routed}/settings"]`) : null
  const gear = routedGear && (await routedGear.count()) ? routedGear : gears.first()
  await gear.click()
  await page.waitForURL((url) => url.pathname.endsWith('/settings'))
  await breadcrumb(page, 'Settings').waitFor()
}

/** Leaves settings for the provider's operations through the breadcrumb. */
export async function closeProviderSettings(page: Page) {
  await breadcrumb(page).getByRole('link').nth(1).click()
  await page.waitForURL((url) => !url.pathname.endsWith('/settings'))
}

/**
 * Disables or enables a provider through Coral, as an operator does: Reef
 * offers no control for it. `providerPath` is the provider's Reef path.
 */
export async function setProviderEnabled(
  accessToken: string,
  providerPath: string,
  enabled: boolean,
) {
  const api = managementClients(accessToken)
  const name = providerPath.replace(/^\//, '')
  const { etag } = await api.providers.getProvider({ name })
  if (enabled) await api.providers.restoreProvider({ name, etag })
  else await api.providers.disableProvider({ name, etag })
}

/**
 * Waits for a provider's rail entry to show its status: its first blocker,
 * else "Connect your account" when only your own account is missing, else
 * "Ready". With `operations`, it also waits for the provider's group heading
 * to show its count and one Unavailable row for each operation Coral cannot
 * call. Name the provider by its display name, or by its path when two
 * providers share one.
 */
export async function waitForProviderStatus(
  page: Page,
  provider: string | { path: string },
  status: string,
  operations?: { available: number; total: number },
) {
  const rail = providersRail(page)
  const id = typeof provider === 'string' ? null : provider.path.split('/').at(-1)
  const entry =
    typeof provider === 'string'
      ? rail.getByRole('link').filter({ has: page.getByText(provider, { exact: true }) })
      : rail.locator(`a[href$="/providers/${id}"]`)
  await entry.getByRole('img', { name: status, exact: true }).first().waitFor()
  const group =
    typeof provider === 'string'
      ? page.getByRole('rowgroup', { name: provider, exact: true })
      : page.locator(`#provider-group-${id}`)
  if (!operations) return
  const { available, total } = operations
  await group
    .getByText(`${total} operation${total === 1 ? '' : 's'}`, { exact: true })
    .first()
    .waitFor()
  await expect
    .poll(() => group.getByText('Unavailable', { exact: true }).count())
    .toBe(total - available)
}

/** Opens one Operation's details drawer from the selected Provider's table. */
export async function openOperation(page: Page, relativePath: string) {
  await page.getByRole('link', { name: relativePath, exact: true }).click()
  const drawer = page.getByRole('dialog', { name: relativePath, exact: true })
  await drawer.waitFor()
  return drawer
}

/**
 * One outcome of the account question, named by what it does rather than by
 * the copy on it: the last step names the two options that use the sign-in in
 * hand differently when there is none to use.
 *
 * - `shared`: one shared account, keeping a sign-in in hand if there is one.
 * - `shared-connect`: one shared account, signing in again for it.
 * - `own`: individual accounts, starting with the sign-in in hand.
 * - `own-connect`: individual accounts, signing in again for your own.
 * - `own-later`: individual accounts, nobody connected yet.
 */
export type AccountOutcome = 'own' | 'own-connect' | 'own-later' | 'shared' | 'shared-connect'

const ACCOUNT_OUTCOMES: Record<AccountOutcome, readonly string[]> = {
  own: ['Connect now with this account', 'Connect now'],
  'own-connect': ['Connect now with another account', 'Connect now'],
  'own-later': ['Connect later'],
  shared: ['Use this account', 'Connect an account'],
  'shared-connect': ['Connect another account', 'Connect an account'],
}

/**
 * Opens the service from the sign-in code dialog, approves the device in the
 * new tab, and closes that tab. The page that started the sign-in stays where
 * it was.
 */
export async function approveDeviceInNewTab(page: Page) {
  const [service] = await Promise.all([
    page.context().waitForEvent('page'),
    page
      .getByRole('dialog')
      .getByRole('button', { name: 'Open sign-in page', exact: true })
      .click(),
  ])
  await service.getByRole('link', { name: 'Approve device access', exact: true }).click()
  await service.getByRole('heading', { name: 'Device approved', exact: true }).waitFor()
  await service.close()
}

/**
 * Checks a radio on a page that a document load has just rendered. Its state
 * lives in React, so a click that lands before hydration changes nothing:
 * click until the choice holds.
 */
export async function checkRadio(radio: Locator) {
  await expect
    .poll(
      async () => {
        await radio.click()
        return radio.isChecked()
      },
      { timeout: 5000 },
    )
    .toBe(true)
}

/** The account question itself, whatever outcomes it currently offers. */
export function accountQuestion(page: Page) {
  return page.getByRole('radiogroup', { name: 'Whose account does it use?' })
}

/** Answers the account question with one of its outcomes. */
export async function chooseAccount(page: Page, outcome: AccountOutcome) {
  await accountQuestion(page).waitFor()
  for (const name of ACCOUNT_OUTCOMES[outcome]) {
    const option = page.getByRole('radio', { name, exact: true })
    if (await option.count()) {
      await checkRadio(option)
      return
    }
  }
  throw new Error(`The account question offers no outcome for ${outcome}`)
}

/**
 * Sets one operation's policy from the provider's operations table: its row's
 * policy toggle has one radio per policy. A saved policy shows in the row
 * itself, so the helper waits for the new radio to stay checked and for the
 * request to release the row.
 */
export async function setOperationPolicy(
  page: Page,
  name: string,
  to: 'Always allow' | 'Needs approval' | 'Disabled',
) {
  const row = page.getByRole('row').filter({
    has: page.getByRole('checkbox', { name: `Select ${name}`, exact: true }),
  })
  const radio = row.getByRole('radio', { name: to, exact: true })
  await radio.click()
  await row.getByRole('radio', { name: to, exact: true, checked: true }).waitFor()
  await expect.poll(() => radio.isEnabled()).toBe(true)
  expect(await row.getByText('Not saved', { exact: true }).count()).toBe(0)
}

/** Enables one operation that still has the default approval policy. */
export async function enableOperation(page: Page, name: string) {
  await setOperationPolicy(page, name, 'Always allow')
}

/**
 * Accepts the listed operations. Every creation attempt shows them between
 * Configuration and the account question; Next is a link, not a submission.
 */
export async function acceptOperations(page: Page) {
  expect(new URL(page.url()).pathname.endsWith('/new')).toBe(true)
  await page.getByRole('heading', { name: 'Operations', exact: true }).waitFor()
  const next = page.getByRole('link', { name: 'Next', exact: true })
  await next.waitFor()
  await next.click()
  await next.waitFor({ state: 'detached' })
  expect(new URL(page.url()).searchParams.get('operationsAccepted')).toBe('1')
}

/**
 * Reads a protected server's operations with a token. The operations step asks
 * for it in a dialog, in the panel the rows will fill.
 */
export async function readOperations(page: Page, secret: string) {
  const dialog = await openDialog(
    page,
    page.getByRole('button', { name: 'Enter a token to read the operations', exact: true }),
  )
  await dialog.getByLabel('Token', { exact: true }).fill(secret)
  await dialog.getByRole('button', { name: 'List operations', exact: true }).click()
}

/**
 * Starts the discovery sign-in. The operations step confirms what the sign-in
 * is for before it leaves for the service, so the dialog repeats the action.
 */
export async function signInToReadOperations(page: Page) {
  const dialog = await openDialog(page, page.getByRole('button', { name: /^Sign in/ }))
  await dialog.getByRole('button', { name: /^Sign in/ }).click()
}

/**
 * Starts the shared account's sign-in from the account step. The step confirms
 * that the workspace shares the account before it leaves for the service.
 */
export async function signInAndShareAccount(page: Page) {
  const dialog = await openDialog(page, page.getByRole('button', { name: /^Sign in/ }))
  await dialog.getByRole('button', { name: 'Sign in and share the account', exact: true }).click()
}

/**
 * Opens a setup step's dialog from its trigger. The step can arrive by a
 * document load, and a click that lands before React attaches opens nothing,
 * so this asks again until the dialog shows.
 */
export async function openDialog(page: Page, trigger: Locator) {
  const dialog = page.getByRole('dialog')
  await expect
    .poll(
      async () => {
        if (!(await dialog.isVisible())) await trigger.click()
        return dialog.isVisible()
      },
      { interval: 250, timeout: 5000 },
    )
    .toBe(true)
  return dialog
}

/**
 * Enters the bearer token for the shared account. The account step asks for it
 * in the confirmation, the same place OAuth confirms before it leaves for the
 * service; an API key would be labelled by its own header.
 */
export async function connectSharedAccount(page: Page, secret: string) {
  const dialog = await openDialog(
    page,
    page.getByRole('button', { name: 'Connect the shared account', exact: true }),
  )
  await dialog.getByLabel('Token', { exact: true }).fill(secret)
  await dialog.getByRole('button', { name: 'Save and share the account', exact: true }).click()
}

/**
 * Configuration needs a namespace before Next. When the source suggests none,
 * the fixture owner supplies one the test can revise in Configuration.
 */
export async function ensureNamespace(page: Page) {
  const namespace = page.getByLabel('Namespace', { exact: true })
  if ((await namespace.count()) && !(await namespace.inputValue())) await namespace.fill('provider')
}

/** The sign-in methods Configuration offers. */
export type SignInMethod =
  | 'No authentication'
  | 'Bearer token'
  | 'API keys in headers'
  | 'Username and password'
  | 'API keys in the query'
  | 'OAuth authorization code'
  | 'OAuth device code'

/** The button that opens the Sign-in menu. It reads the chosen method, or "Choose a method". */
export function signInPicker(page: Page) {
  return page.getByRole('group', { name: 'Sign-in', exact: true }).getByRole('button')
}

/** Chooses a sign-in method from the Sign-in menu. */
export async function chooseSignIn(page: Page, method: SignInMethod) {
  await signInPicker(page).click()
  await page.getByRole('menuitemradio', { name: method, exact: true }).click()
  await expect.poll(() => signInPicker(page).textContent()).toBe(method)
}

/**
 * Chooses OAuth device code for an `oauthApiServer` fixture with `device` set,
 * and fills in its endpoints at `origin`.
 */
export async function chooseDeviceSignIn(page: Page, origin: string) {
  await chooseSignIn(page, 'OAuth device code')
  await page.getByLabel('Device authorization URL', { exact: true }).fill(`${origin}/device`)
  await page.getByLabel('Token URL', { exact: true }).fill(`${origin}/token`)
  await page.getByLabel('Client ID', { exact: true }).fill('manual-client')
}

/** The methods the Sign-in menu lists. It opens the menu to read them, then closes it. */
export async function signInMethods(page: Page) {
  await signInPicker(page).click()
  const items = page.getByRole('menuitemradio')
  await items.first().waitFor()
  const methods = await items.allTextContents()
  await page.keyboard.press('Escape')
  await items.first().waitFor({ state: 'detached' })
  return methods.map((method) => method.trim())
}

/** The Requested scopes input. Typed text becomes one chip per scope. */
export function scopesField(page: Page) {
  return page.getByRole('combobox', { name: 'Requested scopes', exact: true })
}

/** Each scope chip's remove button, named "Remove <scope>", beside the input. */
function scopeRemoveButtons(page: Page) {
  return scopesField(page)
    .locator('xpath=..')
    .getByRole('button', { name: /^Remove / })
}

/** The requested scopes, in chip order. */
export async function requestedScopes(page: Page) {
  await scopesField(page).waitFor()
  const labels = await scopeRemoveButtons(page).evaluateAll((buttons) =>
    buttons.map((button) => button.getAttribute('aria-label') ?? ''),
  )
  return labels.map((label) => label.slice('Remove '.length))
}

/** Removes every scope chip, including those a document or server suggested. */
export async function clearScopes(page: Page) {
  await scopesField(page).waitFor()
  const remove = scopeRemoveButtons(page)
  while (await remove.count()) await remove.first().click()
}

/** Replaces the requested scopes with a typed list, committed by Enter. */
export async function replaceScopes(page: Page, scopes: string[]) {
  await clearScopes(page)
  await scopesField(page).fill(scopes.join(' '))
  await scopesField(page).press('Enter')
  await expect.poll(() => requestedScopes(page)).toEqual(scopes)
}

/**
 * Explicit fixture-owner confirmation; missing document metadata is never consent.
 * Add and a refresh confirm with Next. Edit configuration confirms with Save.
 */
export async function confirmConfiguration(
  page: Page,
  authentication?: SignInMethod,
  submit: 'Next' | 'Save' = 'Next',
) {
  const confirm =
    submit === 'Next'
      ? configurationNext(page)
      : page.getByRole('button', { name: 'Save', exact: true })
  await confirm.waitFor()
  await ensureNamespace(page)
  if (authentication) await chooseSignIn(page, authentication)
  else if ((await signInPicker(page).textContent()) === 'Choose a method')
    await chooseSignIn(page, 'No authentication')
  const clientId = page.getByLabel('Client ID', { exact: true })
  if ((await clientId.count()) && (await clientId.isEditable())) {
    const required = (await clientId.getAttribute('required')) !== null
    if (required && !(await clientId.inputValue())) await clientId.fill('manual-client')
    const callback = page.getByLabel('Callback URL', { exact: true })
    expect(await callback.count()).toBe(1)
    // Reef's own callback, or, without sign-in, Coral's loopback listener.
    expect([
      `${new URL(page.url()).origin}/provider-setup/oauth/callback`,
      expect.stringMatching(/^http:\/\/127\.0\.0\.1:\d+\/provider-oauth\/callback$/),
    ]).toContainEqual(await callback.inputValue())
    await page.getByRole('button', { name: 'Copy callback URL', exact: true }).waitFor()
  } else {
    expect(await page.getByLabel('Callback URL', { exact: true }).count()).toBe(0)
  }
  // Confirm configuration before either discovery or runtime-account authorization.
  // The form posts as a document. Save's next view can show another Save, so
  // the new document, not the button, marks the confirmation.
  const loaded = submit === 'Save' && page.waitForEvent('load')
  await page.getByRole('button', { name: submit, exact: true }).click()
  if (loaded) await loaded
  else await confirm.waitFor({ state: 'detached' })
  expect(await page.getByLabel('Callback URL', { exact: true }).count()).toBe(0)
}

export interface RunUpdate {
  run_id: string
  state: string
  next_cursor: string
  has_more: boolean
  events: {
    kind: 'program.emitted' | 'program.logged'
    sequence: string
    /** Exact RFC 3339 UTC date-time text; keep all nine fractional digits. */
    occurred_at: string
  }[]
  required_actions: {
    kind: 'account_connection' | 'invocation_approval'
    request_id: string
    required_actor: 'invoking_user' | 'workspace_owner'
    /** Exact RFC 3339 UTC date-time text; Date conversion is lossy. */
    deadline_at: string
    run_status_url: string
  }[]
  final_value?: unknown
  failure?: { code: string }
}

interface ToolResult<T = unknown> {
  structuredContent: T
  /** MCP content blocks; rich Program values arrive here, referenced by index. */
  content?: { type: string; text?: string; data?: string; mimeType?: string }[]
  isError?: boolean
}

/**
 * Owns one real MCP session. By default, acquire the browser User's genuine
 * Workspace OAuth grant. Supply an Agent Registration bearer to act as an Agent.
 * Local checks use the implicit User. `wait` observes the original Run without replay.
 * Calling the helper directly retains the serialized result used by older scenarios.
 */
export function programRunner(
  page: Page,
  target: Target,
  workspace: string,
  options: { waitMs?: number; accessToken?: string; taskIntent?: string } = {},
) {
  let id = 0
  const calls: Record<string, number> = {}
  let session = ''
  let taskId = ''
  let accessToken = options.accessToken
  const waitMs = options.waitMs ?? 8000
  const headers = () => ({
    Accept: 'application/json, text/event-stream',
    'MCP-Protocol-Version': '2025-11-25',
    ...(session ? { 'Mcp-Session-Id': session } : {}),
    ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
  })
  /** One JSON-RPC exchange; the body carries either a result or an error. */
  const post = async (method: string, params: object) => {
    if (method === 'tools/call' && 'name' in params && typeof params.name === 'string')
      calls[params.name] = (calls[params.name] ?? 0) + 1
    const response = await page.request.post(`${target.mcpURL}/workspaces/${workspace}/mcp`, {
      headers: headers(),
      data: { jsonrpc: '2.0', id: ++id, method, params },
      // exec and wait hold the request for up to wait_ms, so it must outlast that.
      timeout: waitMs + 10_000,
    })
    expect(response.status(), `MCP ${method} HTTP status`).toBe(200)
    session = response.headers()['mcp-session-id'] ?? session
    const text = await response.text()
    return text.startsWith('{')
      ? JSON.parse(text)
      : JSON.parse(
          text
            .split('\n')
            .find((line) => line.startsWith('data:') && line.slice(5).trim().startsWith('{'))!
            .slice(5),
        )
  }
  const rpc = async (method: string, params: object) => {
    const body = await post(method, params)
    expect(body.error).toBeUndefined()
    return body.result
  }
  const initialize = async () => {
    if (!session) {
      if (!accessToken && target.authentication) accessToken = await mcpToken(page, workspace)
      await rpc('initialize', {
        protocolVersion: '2025-11-25',
        capabilities: {},
        clientInfo: { name: 'Reel', version: '1' },
      })
      await page.request.post(`${target.mcpURL}/workspaces/${workspace}/mcp`, {
        headers: headers(),
        data: { jsonrpc: '2.0', method: 'notifications/initialized' },
      })
    }
    if (!taskId) {
      const task = await rpc('tools/call', {
        name: 'start_task',
        arguments: { intent: options.taskIntent ?? 'Recorded Provider scenario' },
      })
      expect(task.isError).not.toBe(true)
      taskId = task.structuredContent.task_id
    }
  }
  const exec = async (program: string): Promise<ToolResult<RunUpdate>> => {
    await initialize()
    return rpc('tools/call', {
      name: 'exec',
      arguments: {
        task_id: taskId,
        intent: 'Call one Operation',
        program,
        wait_ms: waitMs,
      },
    })
  }
  return Object.assign(async (program: string) => JSON.stringify(await exec(program)), {
    exec,
    calls,
    taskId: () => taskId,
    async endTask(outcome: 'success' | 'failure', feedback?: string): Promise<ToolResult> {
      await initialize()
      return rpc('tools/call', {
        name: 'end_task',
        arguments: { task_id: taskId, outcome, feedback },
      })
    },
    async saveFunction(path: string, description: string, source: string): Promise<ToolResult> {
      await initialize()
      return rpc('tools/call', {
        name: 'save_function',
        arguments: {
          task_id: taskId,
          intent: 'Prepare current work before offboarding',
          path,
          description,
          source,
        },
      })
    },
    async wait(update: RunUpdate): Promise<ToolResult<RunUpdate>> {
      await initialize()
      return rpc('tools/call', {
        name: 'wait',
        arguments: {
          run_id: update.run_id,
          cursor: update.next_cursor || undefined,
          wait_ms: waitMs,
        },
      })
    },
    async search(query?: string): Promise<ToolResult<{ hits: { call_path: string }[] }>> {
      await initialize()
      return rpc('tools/call', {
        name: 'search',
        arguments: { task_id: taskId, intent: 'Find Operations', query },
      })
    },
    async describe(path: string): Promise<ToolResult> {
      await initialize()
      return rpc('tools/call', {
        name: 'describe',
        arguments: { task_id: taskId, intent: 'Inspect one Operation', path },
      })
    },
    /** The tools this MCP session advertises, in order. */
    async listTools(): Promise<{ name: string }[]> {
      await initialize()
      return (await rpc('tools/list', {})).tools
    },
    /** Calls any tool by name, returning the JSON-RPC result or error unchecked. */
    async callTool(
      name: string,
      args: object,
    ): Promise<{ result?: ToolResult; error?: { code: number; message: string } }> {
      await initialize()
      return post('tools/call', { name, arguments: args })
    },
  })
}

/** Collects at most ten more Run Updates without starting or replaying a Program. */
export async function observeRun(
  observer: Pick<ReturnType<typeof programRunner>, 'wait'>,
  initial: ToolResult<RunUpdate>,
) {
  expect(initial.isError).not.toBe(true)
  let update = initial.structuredContent
  const runId = update.run_id
  expect(runId).toBeTruthy()
  const events = [...update.events]
  const responses = [JSON.stringify(initial)]
  for (let pages = 0; pages < 10 && (update.state === 'running' || update.has_more); pages++) {
    const response = await observer.wait(update)
    expect(response.isError).not.toBe(true)
    responses.push(JSON.stringify(response))
    update = response.structuredContent
    expect(update.run_id).toBe(runId)
    events.push(...update.events)
  }
  expect(update.state, JSON.stringify(update.failure)).toBe('succeeded')
  expect(update.has_more).toBe(false)
  return { update, events, responses }
}

/** Replays retained authored output from its beginning through a fresh MCP session. */
export async function observeRetainedRun(
  observer: Pick<ReturnType<typeof programRunner>, 'wait'>,
  original: Pick<Awaited<ReturnType<typeof observeRun>>, 'update' | 'events'>,
) {
  const initial = await observer.wait({ ...original.update, next_cursor: '' })
  expect(initial.isError).not.toBe(true)
  expect(initial.structuredContent.run_id).toBe(original.update.run_id)
  const retained = await observeRun(observer, initial)
  expect(retained.events).toEqual(original.events)
  expect(retained.update.final_value).toEqual(original.update.final_value)
  return retained
}

/** The OAuth settings of a configuration, if it uses authorization-code OAuth. */
export function oauthSettings(configuration: ProviderConfiguration | undefined) {
  return configuration?.authentication.case === 'oauthAuthorizationCode'
    ? configuration.authentication.value
    : undefined
}

async function confirmFixtureConfiguration(
  api: ReturnType<typeof managementClients>,
  setup: ProviderSetup,
) {
  const configuration = setup.configuration?.suggested
  if (!configuration) throw new Error('fixture document has no configuration suggestions')
  if (!configuration.authentication.case)
    configuration.authentication = { case: 'none', value: { $typeName: 'google.protobuf.Empty' } }
  await api.setups.confirmProviderSetupConfiguration({
    name: setup.name,
    expectedRevision: setup.configuration!.revision,
    configuration,
  })
}

/** Publishes one Workspace Provider over the private contract to arrange unrelated prerequisites. */
export async function publishWorkspaceProvider(
  accessToken: string | undefined,
  workspace: string,
  document: Buffer,
  namespace: string,
  credentialRoute = CredentialRoute_Kind.NONE,
) {
  const api = managementClients(accessToken)
  const setup = await api.setups.createProviderSetup({
    parent: workspace,
    input: { case: 'openapiDocument', value: document },
    discoveryCredential: '',
  })
  await confirmFixtureConfiguration(api, setup)
  const publication = await api.providers.publishProvider({
    parent: workspace,
    setup: setup.name,
    requestId: crypto.randomUUID(),
    namespace,
    credentialRoute: { kind: credentialRoute },
    provider: '',
    etag: '',
  })
  if (!publication.provider) throw new Error('fixture publication failed')
  return publication
}

/**
 * Clicks a one-click connect and returns the sign-in progress path Desktop keeps open.
 * It imitates Desktop: Reef names that path in `X-Coral-Authorization-Return-To`
 * on the consent redirect, and Desktop's main process reads, then strips, it.
 */
export async function connectKeepingProgress(page: Page, name: string | RegExp = /^Connect$/) {
  const [posted] = await Promise.all([
    page.waitForResponse(
      (response) =>
        response.request().method() === 'POST' &&
        response.headers()['x-coral-authorization-return-to'] !== undefined,
    ),
    page.getByRole('button', { name }).click(),
  ])
  return posted.headers()['x-coral-authorization-return-to']
}

/** Opens your own account for the workspace's separate-account provider from Accounts. */
export async function openYourAccount(page: Page) {
  const workspace = /^\/workspaces\/([^/]+)/.exec(new URL(page.url()).pathname)![1]
  await page.goto(`/workspaces/${workspace}/accounts`)
  await page.getByRole('heading', { name: 'Accounts', exact: true, level: 1 }).waitFor()
  await page
    .getByRole('link', { name: / account details$/ })
    .first()
    .click()
  await page.waitForURL((url) => /\/accounts\/[^/?#]+$/.test(url.pathname))
}

/** Waits until your account page shows a stored account: its Details card and Reconnect. */
export async function waitForYourAccount(page: Page) {
  await page.getByRole('complementary', { name: 'Details', exact: true }).waitFor()
  await page.getByRole('button', { name: 'Reconnect', exact: true }).waitFor()
}

/** Waits until your account page offers Connect and shows no stored account. */
export async function waitForNoAccount(page: Page) {
  await page.getByRole('button', { name: 'Connect', exact: true }).waitFor()
  expect(await page.getByRole('complementary', { name: 'Details', exact: true }).count()).toBe(0)
}
