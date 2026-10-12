/** Form inputs for Credential Field values are named `credential.<field>`. */
export const CREDENTIAL_INPUT_PREFIX = 'credential.'

export function credentialInputName(field: string) {
  return `${CREDENTIAL_INPUT_PREFIX}${field}`
}
