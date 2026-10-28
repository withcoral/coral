// A document declares servers as URL templates with variables. A person chooses
// a destination, not a template, so the choices are the URLs those templates
// make. Only the variables too numerous or too open to enumerate stay questions.

import type { ConfigurationReviewView, ConfigurationValues } from './types'

export type ServerGroup = ConfigurationReviewView['servers'][number]
export type ServerOption = ServerGroup['options'][number]
export type ServerVariable = ServerOption['variables'][number]
export type ServerSelection = ConfigurationValues['servers'][number]

/** The most URLs one server offers before Coral asks for its variables instead. */
export const URL_LIMIT = 50

const placeholder = /\{([^{}]+)\}/g

/** The URL a template and its variables make. An unanswered variable stays visible. */
export function resolvedUrl(template: string, variables: Record<string, string>) {
  return template.replace(placeholder, (reference, name: string) => variables[name] ?? reference)
}

/** The URL a selection sends requests to, whether it is declared or typed. */
export function selectedUrl(selection: ServerSelection | undefined) {
  if (!selection) return ''
  return selection.custom ? selection.url : resolvedUrl(selection.url, selection.variables)
}

/** One URL a person can choose, and the declared server it comes from. */
export interface ServerUrl {
  url: string
  /** The option's template, which is what the configuration stores. */
  template: string
  variables: Record<string, string>
}

export interface ServerChoices {
  /** Every URL the document declares for this server, resolved. */
  urls: ServerUrl[]
  /** The selected server's variables that the URLs do not answer. */
  open: ServerVariable[]
}

/** A variable Coral can turn into URLs, because the document lists its values. */
function listed(variable: ServerVariable) {
  return variable.enumerated && variable.values.length > 0
}

/** The answers in force for one option: the person's for the one they chose, the document's otherwise. */
function answers(option: ServerOption, selection: ServerSelection | undefined) {
  const saved =
    selection && !selection.custom && selection.url === option.url ? selection.variables : {}
  return Object.fromEntries(
    option.variables.map((variable) => [
      variable.name,
      saved[variable.name] ?? variable.defaultValue,
    ]),
  )
}

/** Every combination of the listed variables, over the answers already in force. */
function combinations(variables: ServerVariable[], base: Record<string, string>) {
  return variables.reduce<Record<string, string>[]>(
    (rows, variable) =>
      rows.flatMap((row) => variable.values.map((value) => ({ ...row, [variable.name]: value }))),
    [base],
  )
}

/**
 * What one server declaration offers: the URLs to choose from, and the variables
 * that stay questions of their own. Two templates can make the same URL, so the
 * first one to make it owns it.
 */
export function serverChoices(
  group: ServerGroup,
  selection: ServerSelection | undefined,
): ServerChoices {
  const total = group.options.reduce(
    (count, option) =>
      count +
      option.variables.filter(listed).reduce((size, variable) => size * variable.values.length, 1),
    0,
  )
  const expand = total <= URL_LIMIT
  const urls: ServerUrl[] = []
  for (const option of group.options) {
    const base = answers(option, selection)
    const rows = expand ? combinations(option.variables.filter(listed), base) : [base]
    for (const variables of rows) {
      const url = resolvedUrl(option.url, variables)
      if (!urls.some((candidate) => candidate.url === url))
        urls.push({ url, template: option.url, variables })
    }
  }
  const chosen = group.options.find((option) => !selection?.custom && option.url === selection?.url)
  return {
    urls,
    open: chosen?.variables.filter((variable) => !expand || !listed(variable)) ?? [],
  }
}

/**
 * A document without a root server has nowhere to send the tools it
 * declares there. Tools with their own server may go unanswered.
 */
export function serverRequired(group: ServerGroup) {
  return group.declaration === '#/servers' && group.tools.length > 0
}

/**
 * Why this server cannot send requests yet, in the words of the choice the
 * person makes. Coral checks the same conditions again and owns the final
 * answer; this one names the field before the step is posted.
 */
export function serverProblem(group: ServerGroup, selection: ServerSelection | undefined) {
  if (!selection) return serverRequired(group) ? 'Choose where Coral sends these tools.' : undefined
  if (selection.custom) return destinationProblem(selection.url)
  const option = group.options.find((choice) => choice.url === selection.url)
  if (!option) return 'This document no longer declares this server. Choose another URL.'
  if (
    Object.keys(selection.variables).some(
      (name) => !option.variables.some((variable) => variable.name === name),
    )
  )
    return 'This document no longer declares these variables. Choose the server URL again.'
  for (const variable of option.variables) {
    const value = selection.variables[variable.name]
    if (value === undefined) return `Enter a value for ${variable.name}.`
    // Match coral-catalog's safe_variable_value, including its UTF-8 byte limit.
    if (new TextEncoder().encode(value).length > 2048)
      return `Enter at most 2048 bytes for ${variable.name}.`
    if (/[\p{Cc}{}?#@\\]/u.test(value))
      return `Remove control characters and { } ? # @ \\ from ${variable.name}.`
    if (variable.enumerated && variable.values.length > 0 && !variable.values.includes(value))
      return `Choose a value this document offers for ${variable.name}.`
  }
  return destinationProblem(selectedUrl(selection))
}

/** Early field feedback; coral-catalog remains the authority for destination validation. */
function destinationProblem(value: string): string | undefined {
  const invalid = 'Enter a full HTTP or HTTPS URL without a query or fragment.'
  if (new TextEncoder().encode(value).length > 2048) return 'Enter a server URL up to 2048 bytes.'
  if (!/^https?:\/\//i.test(value) || /[\p{Cc}{}?#\\]/u.test(value)) return invalid
  try {
    const url = new URL(value)
    if (url.username || url.password) return 'Remove the username and password from the server URL.'
    if (!url.hostname || !['http:', 'https:'].includes(url.protocol)) return invalid
  } catch {
    return invalid
  }
  return undefined
}
