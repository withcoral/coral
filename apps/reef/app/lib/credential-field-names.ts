/** Form inputs for Credential Field values are named `credential.<field>`. */
export const CREDENTIAL_INPUT_PREFIX = 'credential.'

export function credentialInputName(field: string) {
  return `${CREDENTIAL_INPUT_PREFIX}${field}`
}

/** Form inputs for a Secret Reference are named `reference.<field>.<part>`. */
export const REFERENCE_INPUT_PREFIX = 'reference.'

export function referenceInputName(field: string, part: 'store' | 'path' | 'key') {
  return `${REFERENCE_INPUT_PREFIX}${field}.${part}`
}
