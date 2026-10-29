import { useId } from 'react'
import { Combobox, Radio, Typography } from '@/wax/components'

import { serverLabel } from './configuration-details'
import { FieldError, RequiredMark, TextField } from './fields'
import {
  selectedUrl,
  serverChoices,
  serverProblem,
  serverRequired,
  type ServerGroup,
  type ServerSelection,
} from './server-choices'
import * as styles from './providers.css'

/**
 * Where Coral sends calls to one declared server's tools. The field offers the
 * URLs the document declares and accepts a URL it does not; an empty field
 * leaves the server's tools without a destination.
 */
export function ServerField({
  group,
  selection,
  error,
  onChange,
  id: given,
  waitingFor = [],
}: {
  /** The URL input's id, for a step that moves focus to it. */
  id?: string
  /** Labels of the recipe answers this URL is built from that are still missing. */
  waitingFor?: readonly string[]
  group: ServerGroup
  selection: ServerSelection | undefined
  error?: string
  /** null clears the selection. */
  onChange: (selection: ServerSelection | null) => void
}) {
  const generated = useId()
  const id = given ?? generated
  const { urls, open } = serverChoices(group, selection)
  // The named problem first: it says which choice to make, where Coral's own
  // answer covers every way a server can fail at once.
  // A URL still waiting for a recipe answer is fixed in that answer's field.
  const problem = waitingFor.length > 0 ? undefined : (serverProblem(group, selection) ?? error)
  const text = selectedUrl(selection)
  // The combobox replaces text that no item carries when it closes, so a URL the
  // document does not declare joins the list as the person types it.
  const offered = urls.map((candidate) => candidate.url)
  const items = text && !offered.includes(text) ? [...offered, text] : offered

  const choose = (next: string) => {
    if (!next.trim()) return onChange(null)
    const candidate = urls.find((choice) => choice.url === next)
    onChange(
      candidate
        ? {
            declaration: group.declaration,
            url: candidate.template,
            custom: false,
            variables: candidate.variables,
          }
        : {
            declaration: group.declaration,
            url: next,
            custom: true,
            variables: selection?.variables ?? {},
          },
    )
  }
  const answer = (name: string, value: string) =>
    selection && onChange({ ...selection, variables: { ...selection.variables, [name]: value } })

  return (
    <div className={styles.stack}>
      <div className={styles.field}>
        <Typography.BodySmallStrong as="label" htmlFor={id}>
          {serverLabel(group.declaration)}
          {serverRequired(group) && <RequiredMark />}
        </Typography.BodySmallStrong>
        <Combobox.Root
          items={items}
          value={text || null}
          inputValue={text}
          onInputValueChange={choose}
          onValueChange={(next) => typeof next === 'string' && choose(next)}
        >
          <Combobox.Input
            id={id}
            invalid={Boolean(problem)}
            required={serverRequired(group)}
            ariaDescribedBy={problem ? `${id}-error` : undefined}
            maxLength={2048}
            placeholder="https://api.example.com"
          />
          <Combobox.Content>
            <Combobox.Empty />
            <Combobox.List>
              {(item: string) => (
                <Combobox.Item key={item} value={item}>
                  {item}
                </Combobox.Item>
              )}
            </Combobox.List>
          </Combobox.Content>
        </Combobox.Root>
        {waitingFor.length > 0 && (
          <Typography.BodySmall as="p" variant="tertiary">
            Enter a value for {waitingFor.join(' and ')}.
          </Typography.BodySmall>
        )}
        {problem && <FieldError id={`${id}-error`} message={problem} />}
      </div>
      {open.map((variable) =>
        variable.enumerated ? (
          <div className={styles.field} key={variable.name}>
            <Typography.BodySmallStrong as="span">{variable.name}</Typography.BodySmallStrong>
            <Radio.Group
              aria-label={variable.name}
              value={selection?.variables[variable.name] ?? ''}
              onValueChange={(value) => answer(variable.name, value)}
            >
              {variable.values.map((value) => (
                <Radio.Item key={value} value={value}>
                  {value}
                </Radio.Item>
              ))}
            </Radio.Group>
          </div>
        ) : (
          <TextField
            key={variable.name}
            label={variable.name}
            value={selection?.variables[variable.name] ?? ''}
            onChange={(value) => answer(variable.name, value)}
          />
        ),
      )}
    </div>
  )
}
