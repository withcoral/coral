// Why Coral could not learn how to sign in to an MCP server, keyed by the
// discovery code Coral reports. A failed add carries the code as
// AUTHORIZATION_DISCOVERY_<CODE>; a setup that read the catalog stores it as
// its configuration diagnostic. Only codes that discovery alone reports belong
// here: a stored diagnostic carries no provenance, and other steps also report
// outbound_destination_rejected.
const messages: Record<string, string> = {
  resource_metadata_unavailable:
    "Coral cannot read this server's OAuth resource metadata. Check the server URL. If it is correct, contact the server owner.",
  resource_mismatch:
    "This server's OAuth metadata names a different server URL. Check the server URL. If it is correct, contact the server owner.",
  authorization_server_unavailable:
    "Coral cannot read the metadata of this server's authorization server. Try again later. If the problem continues, contact the server owner.",
  issuer_mismatch:
    "The authorization server's metadata names a different issuer. Contact the server owner.",
  pkce_unsupported:
    'The authorization server does not support S256 PKCE. Sign-in needs it. Contact the server owner.',
  insecure_endpoint:
    'This server gives sign-in addresses that do not use HTTPS. Contact the server owner.',
  client_bootstrap_failed:
    'Automatic OAuth client registration does not work with this server. Enter a client ID from the server owner.',
  challenge_unsupported:
    'This server asks for a sign-in method that Coral does not support. Use an API key if the server offers one.',
}

/** One sentence for a discovery code, or undefined when the code is not a discovery failure Reef knows. */
export function authorizationDiscoveryMessage(code: string): string | undefined {
  return Object.hasOwn(messages, code) ? messages[code] : undefined
}
