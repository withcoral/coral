import { useState, type ReactNode } from 'react'
import {
  attemptPath,
  attemptProgress,
  reviseAttempt,
  type CreationAttempt,
} from '@/lib/provider-publication-attempt/model'
import { Button, Typography } from '@/wax/components'
import { PageLayout } from '@/wax/components/page-layout'
import { AddProviderEntry, needsSourceDetails } from './add-provider'
import {
  adHocSource,
  recipeNamespace,
  sourceSearch,
  type SourceChoice,
} from '@/lib/provider-recipes/choice'
import type { AttemptEnd } from '@/lib/provider-publication-attempt/model'
import { AttemptEnded } from './attempt-ended'
import { useReadRecipe } from './read-recipe'
import { NAMESPACE, suggestedNamespace } from './fields'
import { ConfigurationStep } from './configuration-step'
import { ToolsStep } from './tools-step'
import { AccountStep } from './account-step'
import { StepPage } from './step-frame'
import type { FormContext, SetupView, StepperPageProps } from './types'
import * as styles from './providers.css'

/** Remount the steps whenever the attempt or its source changes. */
export function StepperPage(props: StepperPageProps) {
  const source = JSON.stringify([
    props.basePath,
    props.recipe?.kind ?? props.setup?.kind,
    // Bundled documents can share a file name, so the recipe tells them apart.
    // A typed URL matches the source label of the Setup it becomes.
    props.recipe && 'document' in props.recipe
      ? `${props.recipe.id}/${props.recipe.document}`
      : (props.recipe?.url ?? props.setup?.sourceLabel),
  ])
  return (
    <StepperAttemptPage
      key={`${attemptPath(props.basePath, props.attempt)}:${source}`}
      {...props}
    />
  )
}

/** Configuration comes first, followed by account choices and final review. */
function StepperAttemptPage({
  basePath,
  cancelPath,
  setup,
  attempt,
  callbackUrl,
  takenNamespaces = [],
  takenDisplayNames = [],
  recipe = null,
  recipes = [],
  recipeConfiguration = null,
  recipeDocumentationUrl = null,
  form,
  result,
  pending = false,
}: StepperPageProps) {
  const [answers, setAnswers] = useState({
    // A typed URL's host names the provider only until the read lands; then the
    // Setup's document does, unless a recipe suggested a name.
    displayName: attempt.suggestedName || (setup ? undefined : recipe?.displayName),
    namespace:
      attempt.kind === 'workspace'
        ? (result?.configurationDraft?.namespace ??
          (attempt.namespace || (recipe?.id ? recipeNamespace(recipe) : '') || undefined))
        : '',
  })
  // A recipe offers its service. Without one, the document's own name suggests
  // the namespace, then the source host. A host such as
  // raw.githubusercontent.com names where the document lives, not the service.
  // The person confirms it in Configuration.
  const defaultNamespace = recipe?.id
    ? recipeNamespace(recipe)
    : suggestedNamespace(setup?.configuration?.documentDisplayName ?? '') ||
      suggestedNamespace(setup?.sourceHost.split('.')[0] ?? '')
  const shownNamespace = answers.namespace ?? defaultNamespace
  const local: CreationAttempt =
    attempt.state === 'frozen' ? attempt : { ...attempt, namespace: shownNamespace }
  const { stage, ended } = attemptProgress(local, setup, result)
  const configurationPath = attemptPath(basePath, reviseAttempt(local, 'configuration'))
  // Back from the account question returns to the tools, the step before
  // it. Withdrawing the acceptance is what sends the attempt back there.
  const toolsPath = attemptPath(basePath, {
    ...reviseAttempt(local),
    toolsAccepted: false,
  })
  const setNamespace = (namespace: string) => setAnswers((current) => ({ ...current, namespace }))
  const key = `${setup?.id}:${setup?.configuration?.revision}`
  // One step is on screen at a time, and the stage says which. Deciding it once
  // keeps the header and the step from disagreeing about where the person is.
  const content = !setup ? undefined : stage === 'configuration' ? (
    <ConfigurationStep
      key={key}
      setup={setup}
      form={form}
      pending={pending}
      result={result}
      callbackUrl={callbackUrl}
      namespace={local.namespace}
      onNamespace={setNamespace}
      defaultNamespace={defaultNamespace}
      takenNamespaces={takenNamespaces}
      takenDisplayNames={takenDisplayNames}
      suggestedName={answers.displayName ?? local.suggestedName}
      recipe={recipeConfiguration}
      retry={<Button.InternalLink to={basePath}>Choose a source</Button.InternalLink>}
    />
  ) : stage === 'discovery' || stage === 'tools' ? (
    // Discovery is this same step without a list yet: it asks for the sign-in
    // that produces one, in the panel the list will fill.
    <ToolsStep
      key={key}
      attempt={local}
      setup={setup}
      basePath={basePath}
      backPath={configurationPath}
      discovering={stage === 'discovery'}
      form={form}
      pending={pending}
      result={result}
      callbackUrl={callbackUrl}
    />
  ) : stage === 'account' || stage === 'publish' ? (
    // The account question and the button that acts on it are one step: the
    // answer states what Add commits rather than moving to a page that only
    // restates it. Remounted per stage, so the question's own state does not
    // outlive the answer.
    <AccountStep
      key={`${key}:${stage}`}
      attempt={local}
      setup={setup}
      basePath={basePath}
      form={form}
      result={result}
      pending={pending}
      callbackUrl={callbackUrl}
      backPath={toolsPath}
      documentationUrl={recipeDocumentationUrl}
      disabled={local.kind === 'workspace' && !NAMESPACE.test(local.namespace)}
    />
  ) : undefined
  const providerName = setup?.configuration?.confirmed
    ? setup.configuration.values.displayName
    : local.suggestedName || setup?.configuration?.values.displayName || recipe?.displayName
  // One page for every state. A step heads its content with its title and actions.
  const page = (body: ReactNode) => (
    <PageLayout
      variant="wide"
      breadcrumbs={[
        { id: 'providers', type: 'link', text: 'Providers', to: cancelPath },
        { id: 'add', type: 'text', text: `Add ${providerName || 'provider'}` },
      ]}
    >
      {body}
    </PageLayout>
  )
  if (ended && setup)
    return page(
      <AddEnded
        ended={ended}
        setup={setup}
        recipe={recipe}
        form={form}
        basePath={basePath}
        cancelPath={cancelPath}
      />,
    )
  if (content)
    return page(
      <div className={styles.setupPage}>
        <StepPage.Provider value>{content}</StepPage.Provider>
      </div>,
    )
  return page(
    <div className={styles.setupPage}>
      {setup ? null : (
        <section className={styles.stack} aria-label="Source">
          <Typography.HeadingSmall as="h2">Source</Typography.HeadingSmall>
          <AddProviderEntry
            form={form}
            pending={pending}
            result={result}
            newPath={basePath}
            recipes={recipes}
            initialRecipe={recipe}
          />
        </section>
      )}
    </div>,
  )
}

/**
 * A Setup that ended. Start again reads the same source into a new Setup, as
 * the catalog does. A source that needs details or an upload opens the source
 * form instead, and so does Choose a source after a read fails.
 */
function AddEnded({
  ended,
  setup,
  recipe,
  form,
  basePath,
  cancelPath,
}: {
  ended: AttemptEnd
  setup: SetupView
  recipe: SourceChoice | null
  form: FormContext
  basePath: string
  cancelPath: string
}) {
  const { read, reading, failed } = useReadRecipe(form, basePath)
  // A typed URL keeps no source choice in the address, but its Setup names the URL.
  const source =
    recipe ??
    (setup.sourceHost
      ? adHocSource(setup.kind === 'mcp' ? 'mcp' : 'openapi', setup.sourceLabel)
      : null)
  const leave = (
    <Button.InternalLink to={cancelPath} variant="secondary">
      Back to providers
    </Button.InternalLink>
  )
  if (!source)
    return (
      <AttemptEnded ended={ended} description="Upload the document again to continue.">
        {leave}
        <Button.InternalLink to={basePath}>Choose a source</Button.InternalLink>
      </AttemptEnded>
    )
  // A read that fails says why in a toast. The source form lets you correct the source.
  return (
    <AttemptEnded ended={ended}>
      {leave}
      {failed && (
        <Button.InternalLink to={basePath} variant="secondary">
          Choose a source
        </Button.InternalLink>
      )}
      {needsSourceDetails(source) ? (
        <Button.InternalLink to={`${basePath}?${sourceSearch(source)}`}>
          Start again
        </Button.InternalLink>
      ) : (
        <Button.TextButton
          type="button"
          disabled={Boolean(reading)}
          busy={Boolean(reading)}
          onClick={() => read(source)}
        >
          Start again
        </Button.TextButton>
      )}
    </AttemptEnded>
  )
}
