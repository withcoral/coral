import { Form } from 'react-router'
import { Banner, Inputs, Typography } from '@/wax/components'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { Fields } from '@/views/management/forms'
import { Disclosure } from '@/wax/components/disclosure'
import { useArrivalToast } from '@/lib/use-arrival-toast'
import { UpdateSource } from './update-source'
import type { UpdatePageProps } from './types'
import { StepPage } from './step-frame'
import { DocumentSubmitButton } from '@/components/document-submit-button'
import * as styles from './providers.css'

/**
 * Check for updates before the source is read, or Replace source. Once the
 * source is read, the edit page takes over.
 */
export function UpdatePage({
  application,
  applicationProblem,
  displayName,
  basePath,
  cancelPath,
  settingsPath = `${cancelPath}/settings`,
  providersPath,
  attempt,
  form,
  result,
  source,
  replacingSource = false,
  pending = false,
}: Omit<UpdatePageProps, 'setup'>) {
  // Replacing the client secret reloads this page with `secretRotated` in the query.
  useArrivalToast(SECRET_ROTATED, () => ({
    title: 'Application secret updated',
    description: 'Your connected accounts keep their authorization.',
  }))
  return (
    <PageLayout
      variant="wide"
      breadcrumbs={trail(
        [
          { label: 'Providers', to: providersPath },
          { label: displayName, to: cancelPath },
          { label: 'Settings', to: settingsPath },
        ],
        replacingSource ? 'Replace source' : 'Check for updates',
      )}
    >
      <div className={styles.setupPage}>
        <StepPage.Provider value>
          <UpdateSource
            source={source}
            replacingSource={replacingSource}
            basePath={basePath}
            cancelPath={settingsPath}
            attempt={attempt}
            form={form}
            pending={pending}
            result={result}
          >
            {applicationProblem && (
              <Banner variant="error">
                The service rejected the OAuth application. Repair its configuration before you
                reconnect accounts.
              </Banner>
            )}
            {application?.secretConfigured && (
              <Disclosure title="OAuth application secret">
                {application.problem && (
                  <Banner variant="error">
                    The service rejected the application credentials. Replace the client secret to
                    restore access.
                  </Banner>
                )}
                <Form method="post" reloadDocument className={styles.stack}>
                  <Fields form={form} intent="rotate-client-secret" />
                  <input type="hidden" name="clientId" value={application.clientId} />
                  <Typography.BodySmall as="p">
                    Client ID: {application.clientId}
                  </Typography.BodySmall>
                  <Typography.BodySmallStrong as="label" htmlFor="replacement-client-secret">
                    Replacement client secret
                  </Typography.BodySmallStrong>
                  <Inputs.CredentialInput id="replacement-client-secret" name="clientSecret" />
                  <DocumentSubmitButton>Replace secret</DocumentSubmitButton>
                </Form>
              </Disclosure>
            )}
          </UpdateSource>
        </StepPage.Provider>
      </div>
    </PageLayout>
  )
}

const SECRET_ROTATED = ['secretRotated'] as const
