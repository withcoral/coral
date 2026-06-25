import { Shell } from '@/components/shell'
import { useRouter } from '@/lib/router'
import { TracesPage } from '@/views/TracesPage'
import { SettingsPage } from '@/views/settings/settings-page'
import { SourcesIndex } from '@/views/sources/sources-index'
import { ToastContainer } from '@/wax/components/toast'
import { useThemeClassOnBody } from '@/wax/theme/theme-provider'
import '@/app.css'

function CurrentRoute() {
  const { location } = useRouter()

  switch (location.route.kind) {
    case 'sources':
      return <SourcesIndex />
    case 'settings':
      return <SettingsPage />
    case 'traces':
      return <TracesPage />
  }
}

export function App() {
  useThemeClassOnBody()

  return (
    <Shell>
      <CurrentRoute />
      <ToastContainer />
    </Shell>
  )
}
