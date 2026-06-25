import type { Route } from './+types/sources'

import { Typography } from '@/wax/components/typography'

export { action } from './sources-action.server'
export type { SourceActionIntent, SourcesActionData } from './sources-action.server'
export { loader } from './sources-loader.server'
export type { SelectedSource, SourcesLoaderData } from './sources-loader.server'

export default function SourcesRoute({ loaderData }: Route.ComponentProps) {
  return (
    <>
      <Typography.HeadingLarge as="h1">Sources</Typography.HeadingLarge>
      {loaderData.loadError ? (
        <Typography.BodySmall variant="secondary">
          Couldn't load sources: {loaderData.loadError}
        </Typography.BodySmall>
      ) : null}
    </>
  )
}
