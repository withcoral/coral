import { Shell } from '@/components/shell'
import { useRouter } from '@/lib/router'
import { TracesPage } from '@/views/TracesPage'
import { SourcesIndex } from '@/views/sources/sources-index'
import { ToastContainer } from '@/wax/components/toast'
import { useThemeClassOnBody } from '@/wax/theme/theme-provider'
import '@/app.css'

export function App() {
  useThemeClassOnBody()
  const { location } = useRouter()

  return (
    <Shell>
      {location.route.kind === 'sources' ? <SourcesIndex /> : <TracesPage />}
      <ToastContainer />
    </Shell>
  )
}
