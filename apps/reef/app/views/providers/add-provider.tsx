import { useEffect, useId, useRef, useState, type ReactNode } from 'react'
import { Button } from '@/wax/components'
import type { ButtonVariant } from '@/wax/components/button/button.css'
import * as Dialog from '@/wax/components/dialog'
import { sourceSearch, type SourceChoice } from '@/lib/provider-recipes/choice'
import { ProviderCatalog } from './provider-catalog'
import { SourceStep, type SourceTab } from './source-step'
import { useReadRecipe } from './read-recipe'
import type { FormContext, StepperResult } from './types'
import * as styles from './providers.css'

export interface AddProviderEntryProps {
  form: FormContext
  pending: boolean
  /** The add page for this scope; every read starts or lands there. */
  newPath: string
  result?: StepperResult
  /** Recipes the catalog offers, from the bundled library. */
  recipes?: readonly SourceChoice[]
  /** A source selected by the add page's recipe or URL query parameters. */
  initialRecipe?: SourceChoice | null
  /** The catalog's search when it opens. */
  catalogQuery?: string
  /** The tab the entry opens on. */
  initialTab?: SourceTab
  /**
   * The caller is a dialog that bounds the entry's height: the source form
   * keeps its size, the catalog scrolls in what is left, and leaving a chosen
   * recipe moves to the pinned footer.
   */
  fill?: boolean
  /** Told when any read starts and ends, so a dialog can stay open for it. */
  onReading?: (reading: boolean) => void
}

/** Source details must be answered before a catalog recipe can be read. */
export function needsSourceDetails(recipe: SourceChoice) {
  return (
    Boolean(recipe.sourceInputs?.length) ||
    (recipe.requestHeaders?.some((header) => !header.value) ?? false)
  )
}

/**
 * Everything "Add provider" starts from: the source form, then the catalog.
 * The same entry serves the launcher dialog, the empty state, and the add page.
 * A typed address, a file, or a catalog recipe is read where it is chosen, and
 * only a read that succeeds moves on to Configuration. A refusal stays here, so
 * the person can choose another at once.
 */
export function AddProviderEntry({
  form,
  pending,
  newPath,
  result,
  recipes = [],
  initialRecipe = null,
  catalogQuery,
  initialTab = 'catalog',
  fill = false,
  onReading,
}: AddProviderEntryProps) {
  const sourceForm = useId()
  const [tab, setTab] = useState<SourceTab>(initialTab)
  const [chosen, setChosen] = useState<SourceChoice | null>(initialRecipe)
  const { read, reading } = useReadRecipe(form, newPath)
  // The catalog reads a recipe here; the source form reads what it holds itself.
  const [formReading, setFormReading] = useState(false)
  const busy = Boolean(reading) || formReading
  useEffect(() => {
    onReading?.(busy)
    // An entry that goes away mid-read must not leave its dialog held open.
    return () => onReading?.(false)
  }, [busy, onReading])
  const back = (
    <Button.TextButton
      type="button"
      variant="secondary"
      disabled={formReading}
      onClick={() => setChosen(null)}
    >
      Back
    </Button.TextButton>
  )
  if (chosen)
    return (
      <>
        <SourceStep
          key={chosen.id}
          form={form}
          pending={pending}
          action={`${newPath}?${sourceSearch(chosen)}`}
          recipe={chosen}
          formId={sourceForm}
          fill={fill}
          onReading={setFormReading}
        />
        {fill ? (
          <Dialog.Actions>{back}</Dialog.Actions>
        ) : (
          <div className={styles.stepActions}>{back}</div>
        )}
      </>
    )
  return (
    <SourceStep
      form={form}
      pending={pending}
      result={result}
      action={newPath}
      formId={fill ? sourceForm : undefined}
      fill={fill}
      tab={tab}
      onTabChange={setTab}
      onReading={setFormReading}
      catalog={
        <section className={fill ? styles.catalogPane : styles.stack} aria-label="Catalog">
          <ProviderCatalog
            recipes={recipes}
            reading={reading}
            onRecipe={(recipe) => (needsSourceDetails(recipe) ? setChosen(recipe) : read(recipe))}
            onCustom={() => setTab('custom')}
            scroll={fill}
            initialQuery={catalogQuery}
          />
        </section>
      }
    />
  )
}

/**
 * The "Add provider" button and the dialog it opens, holding the entry above.
 */
export function AddProviderLauncher({
  description,
  labelClassName,
  variant = 'primary',
  label = 'Add provider',
  leading = <Button.Icon name="Plus" />,
  ...entry
}: Omit<AddProviderEntryProps, 'result' | 'onReading'> & {
  description?: string
  /** Styles the button's label, such as a toolbar's `toolbarActionLabel`. */
  labelClassName?: string
  variant?: ButtonVariant
  label?: string
  /** Leads the label: a plus by default, or the logo of the provider it adds. */
  leading?: ReactNode
}) {
  const [open, setOpen] = useState(false)
  // A catalog read in flight keeps the dialog open: it moves on by itself when
  // the read succeeds, and says why here when it does not.
  const [readingRecipe, setReadingRecipe] = useState(false)
  const popup = useRef<HTMLDivElement>(null)
  return (
    <Dialog.Root
      open={open}
      onOpenChange={(next) => {
        if (next || !readingRecipe) setOpen(next)
      }}
    >
      <Dialog.Trigger render={<Button.Container variant={variant} size="32" hasPrefix />}>
        {leading}
        <Button.Text className={labelClassName} tooltip={label}>
          {label}
        </Button.Text>
      </Dialog.Trigger>
      {open && (
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup
            ref={popup}
            size="l"
            className={styles.addProviderPopup}
            // The dialog opens on the catalog, so typing goes to its search.
            initialFocus={() =>
              popup.current?.querySelector<HTMLInputElement>('input[type="search"]') ?? true
            }
          >
            <Dialog.Title>Add a provider</Dialog.Title>
            {/* Only a scope whose dialog carries a fact nothing below it states.
                The field names the formats it takes and the catalog heads
                itself, so the Workspace dialog says neither again. */}
            {description && <Dialog.Description>{description}</Dialog.Description>}
            <AddProviderEntry {...entry} fill onReading={setReadingRecipe} />
          </Dialog.Popup>
        </Dialog.Portal>
      )}
    </Dialog.Root>
  )
}
