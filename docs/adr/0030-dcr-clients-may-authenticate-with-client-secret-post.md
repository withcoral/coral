---
status: accepted
---

# DCR clients may authenticate with `client_secret_post`

Coral's authorization server accepted only public OAuth Clients. Slackbot registers by Dynamic Client Registration as
a `client_secret_post` client, so it could not sign in to Coral's MCP endpoint. Slackbot registers again only when the
server URL changes, and we cannot make it pick CIMD.

Dynamic Client Registration now accepts public clients and `client_secret_post` clients. A confidential client is shown
its secret once, at registration, and Coral keeps only a verifier. This reverses the "public clients only" rule. CIMD
clients stay public. [#2823](https://github.com/withcoral/coral/issues/2823) holds the details.

## Decisions

- **Strict per-client matching.** A client authenticates by exactly the method it registered. A public client that
  sends any secret is refused. A confidential client that sends no secret, the wrong one, or another kind of credential
  is refused. An unknown client is refused the same way, so a refusal discloses nothing.
- **Authenticate before anything spends.** Every endpoint that takes a code or a refresh token authenticates the client
  first. A failed check spends no code and ends no Refresh Grant, so a leaked refresh token without the secret can
  neither refresh nor revoke (RFC 6749 section 6, RFC 7009 section 2.1).
- **Web sessions authenticate too.** Any client can obtain a Refresh Grant for Reef's audience. The web-session
  endpoints therefore check the client like the token endpoint does, or a leaked confidential refresh token could start
  a session without the secret. Reef is public, so nothing changes for it.
- **No secret lifecycle.** A secret does not expire or rotate, and there is no RFC 7592 management endpoint. A client
  that loses its secret registers again, against the shared registration capacity.
- **No new trust.** A secret proves the same software is calling back, not who it is. Consent, the self-asserted client
  label and trusted clients are unchanged, and the secret is never a Coral credential.

## The seam

The OAuth client module in `coral-auth` owns every authentication rule. The HTTP handlers only parse a request into one
presented credential and refuse a request that carries more than one (RFC 6749 section 2.3). Authentication returns
evidence that only the module can make, and every operation that spends a code or a refresh token, or ends a grant,
requires it. So no endpoint can reach those operations before the check.
`private_key_jwt` for CIMD clients ([#2666](https://github.com/withcoral/coral/issues/2666)) joins this seam as one more
presented credential, with its rule beside the others.

## Rejected

- **Make Slackbot use CIMD.** Slack publishes a metadata document that declares a public client, but Slackbot chooses
  DCR and we cannot change that.
- **Accept `client_secret_post` and record a public client.** Coral would tell the client one method and enforce
  another, and a leaked refresh token would work without the secret it was promised to need.
- **Slack's manual OAuth mode.** Each workspace administrator would copy a client ID and secret into Slack by hand.
- **Also accept `client_secret_basic`.** No client we support needs it. Strict matching refuses it until one does.
