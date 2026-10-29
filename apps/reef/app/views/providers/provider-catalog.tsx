import { useEffect, useId, useRef, useState } from 'react'
import { Button, Card, Inputs, ScrollArea, Typography } from '@/wax/components'
import { Logo } from '@/wax/components/logo'
import {
  byService,
  type RecipeIcons,
  type ServiceChoices,
  type SourceChoice,
} from '@/lib/provider-recipes/choice'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import { filterQuery, type FilterQuery } from '@/utils/filter-query'
import * as styles from './providers.css'

/**
 * One service to add. The card names it and holds a button for each of its
 * sources, MCP before API, so every card asks the same question the same way.
 * A click elsewhere on the card adds the first source, and the button a click
 * would press looks pressed while the pointer is on the card. The buttons stay
 * the keyboard's way in, so the card itself is only a larger pointer target.
 */
function ServiceRow({
  name,
  icons,
  choices,
  reading,
  disabled,
  onRecipe,
}: {
  name: string
  icons?: RecipeIcons
  choices: readonly SourceChoice[]
  /** The recipe Coral is reading, when it is one of this card's. */
  reading?: SourceChoice
  disabled: boolean
  onRecipe: (recipe: SourceChoice) => void
}) {
  const id = useId()
  const sources = choices.toSorted((a, b) => Number(b.kind === 'mcp') - Number(a.kind === 'mcp'))
  const first = sources[0]
  // The source a click would add now: the hovered button's, else the first.
  const [target, setTarget] = useState<string | null>(null)
  return (
    <li className={styles.catalogItem}>
      <Card.Card
        className={styles.catalogCard}
        compact
        interactive={!disabled}
        title={name}
        icon={<Logo name={name} icons={icons} size="40" />}
        onPointerEnter={() => setTarget(first.id)}
        onPointerLeave={() => setTarget(null)}
        onClick={(event: React.MouseEvent) => {
          if (disabled || (event.target as Element).closest('button')) return
          onRecipe(first)
        }}
        action={sources.map((choice) => {
          const source = choice.kind === 'mcp' ? 'MCP' : 'API'
          const busy = reading?.id === choice.id
          return (
            <Button.Container
              key={choice.id}
              type="button"
              variant="outline"
              disabled={disabled}
              isActive={!disabled && target === choice.id}
              ariaLabel={`Add ${name} ${source}`}
              aria-busy={busy ? true : undefined}
              aria-describedby={`${id}-${choice.id}`}
              onPointerEnter={() => setTarget(choice.id)}
              onPointerLeave={() => setTarget(first.id)}
              onClick={() => onRecipe(choice)}
            >
              {busy && <Button.SpinningButtonIcon name="Loader" />}
              <Button.Text>{source}</Button.Text>
            </Button.Container>
          )
        })}
      />
      {/* Each button shows only its source, so what that source offers is read out here. */}
      {choices.map((choice) => (
        <span className={visuallyHidden} id={`${id}-${choice.id}`} key={choice.id}>
          {choice.description}
        </span>
      ))}
    </li>
  )
}

/**
 * The rows themselves. In a dialog they are the one thing that scrolls, in the
 * Wax scroll area so the fade says the list continues past the edge.
 */
function Rows({ scroll, children }: { scroll: boolean; children: React.ReactNode }) {
  if (!scroll) return <div className={styles.stack}>{children}</div>
  return (
    <ScrollArea.Container
      className={styles.catalogScroll}
      constrainWidth
      fade="bottom"
      height="auto"
      scrollDirection="vertical"
    >
      <div className={styles.catalogScrollContent}>{children}</div>
    </ScrollArea.Container>
  )
}

/**
 * The catalog's services that `query` matches. Only what the card shows, so
 * every match explains itself: a service matches by any of its recipes' names
 * and ids, and then offers them all.
 */
export function catalogMatches(services: readonly ServiceChoices[], query: FilterQuery) {
  return services.filter(({ choices }) =>
    query.matches(...choices.flatMap((recipe) => [recipe.displayName, recipe.id])),
  )
}

function providerCount(count: number) {
  return `${count} ${count === 1 ? 'provider' : 'providers'}`
}

/** Bundled sources start an independent Provider setup. */
export function ProviderCatalog({
  recipes,
  reading = null,
  onRecipe,
  onCustom,
  scroll = false,
  initialQuery = '',
}: {
  recipes: readonly SourceChoice[]
  /** The recipe Coral is reading; every row waits for it. */
  reading?: SourceChoice | null
  onRecipe: (recipe: SourceChoice) => void
  /** Leaves the catalog to add a provider it does not list, by its address. */
  onCustom?: () => void
  /** Take the height the caller leaves and scroll the rows inside it. */
  scroll?: boolean
  /** The search the catalog opens with, such as a filter that found no installed provider. */
  initialQuery?: string
}) {
  const [query, setQuery] = useState(initialQuery)
  const search = useRef<HTMLInputElement>(null)
  const catalogQuery = filterQuery(query)
  const services = byService(recipes)
  const serviceMatches = catalogMatches(services, catalogQuery)
  const nothingMatches = serviceMatches.length === 0
  // Find-in-page cannot reach rows the catalog is scrolling past, so the
  // shortcut people already press goes to the search that can.
  useEffect(() => {
    const focusSearch = (event: KeyboardEvent) => {
      if (event.key !== 'f' || event.altKey || !(event.metaKey || event.ctrlKey)) return
      event.preventDefault()
      search.current?.focus()
      search.current?.select()
    }
    document.addEventListener('keydown', focusSearch)
    return () => document.removeEventListener('keydown', focusSearch)
  }, [])
  return (
    <div className={scroll ? styles.catalogPane : styles.stack}>
      <div className={styles.catalogToolbar}>
        <Typography.BodyStrong as="p" variant="primary">
          Choose a provider from the list
          {onCustom && (
            <>
              {' or '}
              <Button.TextButton type="button" variant="link" size="32" onClick={onCustom}>
                add a custom one
              </Button.TextButton>
            </>
          )}
        </Typography.BodyStrong>
        <div className={styles.catalogFind}>
          {/* How many there are, and while searching how many match. The empty
              state speaks for itself, so the count stays quiet then. */}
          <Typography.BodySmall
            as="p"
            variant="tertiary"
            aria-live="polite"
            className={styles.catalogCount}
          >
            {nothingMatches
              ? ''
              : catalogQuery.text
                ? `${serviceMatches.length} of ${providerCount(services.length)}`
                : providerCount(services.length)}
          </Typography.BodySmall>
          <div className={styles.catalogSearch}>
            <Inputs.TextInput
              ref={search}
              ariaLabel="Search the catalog"
              type="search"
              icon="Search"
              placeholder="Search the catalog…"
              value={query}
              onChange={setQuery}
            />
          </div>
        </div>
      </div>
      {nothingMatches ? (
        // Centered in the space the rows would take, so it reads as the result.
        <div className={styles.catalogEmpty} role="status">
          <Typography.BodyStrong as="p" variant="primary">
            No providers match “{query.trim()}”
          </Typography.BodyStrong>
          <Typography.BodySmall as="p" variant="tertiary">
            Try another name. You can also add any provider from its MCP server or OpenAPI URL.
          </Typography.BodySmall>
          <div className={styles.catalogEmptyActions}>
            <Button.TextButton
              type="button"
              variant="secondary"
              onClick={() => {
                setQuery('')
                search.current?.focus()
              }}
            >
              Clear search
            </Button.TextButton>
            {onCustom && (
              <Button.TextButton type="button" onClick={onCustom}>
                Add custom provider
              </Button.TextButton>
            )}
          </div>
        </div>
      ) : (
        <Rows scroll={scroll}>
          {/* No visible heading: the rows are the default group, so the label is
              only worth the space for assistive technology. */}
          <section className={styles.stack} aria-label="Recipes">
            <ul className={styles.catalogList}>
              {serviceMatches.map(({ service, shown, choices }) => (
                <ServiceRow
                  key={service}
                  name={shown.displayName}
                  icons={shown.icons}
                  choices={choices}
                  reading={choices.find((choice) => choice.id === reading?.id)}
                  disabled={Boolean(reading)}
                  onRecipe={onRecipe}
                />
              ))}
            </ul>
          </section>
        </Rows>
      )}
    </div>
  )
}
