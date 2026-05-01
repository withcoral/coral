import { Shell } from '@/components/shell'
import { SchemaExplorer } from '@/views/SchemaExplorer'
import { useThemeClassOnBody } from '@/wax/theme/theme-provider'
import '@/app.css'

export function App() {
  useThemeClassOnBody()

  return (
    <Shell>
      <SchemaExplorer />
    </Shell>
  )
}
