/**
 * Liveness only reports that the process serves. Startup already refused an
 * invalid configuration, and readiness follows Coral.
 */
export function loader(): Response {
  return Response.json({ status: 'ok' })
}
