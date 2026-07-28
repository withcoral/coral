import { useState } from 'react'
import { useRevalidator } from 'react-router'

import { ErrorBanner } from '@/components/error-banner'
import { FunctionExplorer, type FunctionDetailsProps } from '@/components/functions'
import { PageHeader } from '@/views/traces/page-header'

import * as styles from './functions-index.css'

export function FunctionsIndex({
  functions,
  loadError,
}: {
  functions: FunctionDetailsProps[]
  loadError: string | null
}) {
  const revalidator = useRevalidator()
  const [selectedName, setSelectedName] = useState<string>()
  const activeName = functions.some((fn) => fn.name === selectedName)
    ? selectedName
    : functions[0]?.name

  if (!loadError) {
    return (
      <FunctionExplorer
        functions={functions}
        onSelect={setSelectedName}
        selectedName={activeName}
      />
    )
  }

  return (
    <section aria-label="Functions" className={styles.root}>
      <PageHeader title="Functions" />
      <div className={styles.error}>
        <ErrorBanner
          message={loadError}
          onRetry={() => revalidator.revalidate()}
          title="Couldn't load functions"
        />
      </div>
    </section>
  )
}
