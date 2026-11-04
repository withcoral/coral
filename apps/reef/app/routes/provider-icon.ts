import type { Route } from './+types/provider-icon'
import {
  management,
  managementFailure,
  privateHeaders,
  workspaceName,
} from '@/lib/management.server'
import { providerName } from '@/lib/providers.server'

// An uploaded SVG is untrusted markup. An <img> never runs its script, and these
// headers keep a browser that opens the URL itself from running it either.
const imageHeaders = {
  'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; sandbox",
  'X-Content-Type-Options': 'nosniff',
}

/**
 * The icon an owner uploaded for a provider, for anyone who can see the provider. The
 * protected layout marks every response `no-store`, so the browser reads the icon again
 * on each page load. Pages name the icon's digest in `v`: a new icon gets a new URL, so
 * no image the page already holds stands in for it.
 */
export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const icon = await management(context).providers.getProviderIcon(
      { name: providerName(workspaceName(params.workspace), params.provider) },
      { signal: request.signal },
    )
    // A copy, so the body owns a plain ArrayBuffer as `Response` requires.
    return new Response(new Uint8Array(icon.content), {
      headers: {
        ...imageHeaders,
        'Content-Type': icon.mediaType,
        ...privateHeaders,
      },
    })
  } catch (error) {
    // A provider without an uploaded icon reads as not found.
    managementFailure(error)
  }
}
