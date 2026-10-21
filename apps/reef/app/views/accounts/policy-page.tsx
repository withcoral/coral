import { useState } from 'react'
import { Form } from 'react-router'
import { Banner, Button, Inputs, Radio, Typography } from '@/wax/components'
import { Pill } from '@/wax/components/pill'
import { policyLabels, policyOptions, type PolicyState } from '@/components/policy-toggle'
import { Fields, ActionNotice } from '@/views/management/forms'
import { ManagementLayout, trail } from '@/views/management/layout'
import type { FormContext, ActionResult } from './types'
import type { Crumb, WorkspaceView } from '@/views/management/types'
import * as styles from './accounts.css'

export type AccessState = PolicyState
export interface PolicyView {
  workspace?: WorkspaceView
  parents: Crumb[]
  canChange: boolean
  operations: {
    name: string
    path: string
    state: AccessState
    explicit: boolean
    available: boolean
  }[]
  next: string
  form: FormContext
}
export function PolicyPage({
  view: v,
  result,
  pending = false,
}: {
  view: PolicyView
  result?: ActionResult
  pending?: boolean
}) {
  const [access, setAccess] = useState<AccessState>('approval_required')
  return (
    <ManagementLayout breadcrumbs={trail(v.parents, 'Operation policy')}>
      <ActionNotice result={result} />
      <Typography.BodySmall as="p">
        Select up to 100 exact operations. Coral saves the selection together or changes nothing.
      </Typography.BodySmall>
      <Typography.BodySmall as="p">
        These rules affect every caller through this namespace. Other providers keep their own
        rules.
      </Typography.BodySmall>
      {!v.canChange && (
        <Banner variant="info">Only owners can change policy in an active workspace.</Banner>
      )}
      <Form method="post" className={styles.stack}>
        <Fields form={v.form} intent="policy" />
        {v.operations.map((operation) => (
          <div className={styles.account} key={operation.name}>
            <Inputs.Checkbox
              name="operation"
              value={operation.name}
              disabled={!v.canChange || pending}
            >
              {operation.path}
            </Inputs.Checkbox>
            <Pill color={operation.state === 'disabled' ? 'amber' : 'gray'}>
              {policyLabels[operation.state]} · {operation.explicit ? 'Exact rule' : 'Default'}
            </Pill>
            {!operation.available && (
              <Typography.BodySmall>
                Unavailable. Changing policy does not make this operation available.
              </Typography.BodySmall>
            )}
          </div>
        ))}
        {v.canChange && v.operations.length > 0 && (
          <>
            <Typography.BodySmallStrong as="span">
              Access for selected operations
            </Typography.BodySmallStrong>
            <input type="hidden" name="access" value={access} />
            <Radio.Group value={access} onValueChange={(value) => setAccess(value as AccessState)}>
              {policyOptions.map(({ value }) => (
                <Radio.Item key={value} value={value}>
                  {policyLabels[value]}
                </Radio.Item>
              ))}
            </Radio.Group>
            <Button.TextButton
              type="submit"
              disabled={pending}
              busyStatus={pending ? 'Saving…' : undefined}
            >
              Save selected operations
            </Button.TextButton>
          </>
        )}
      </Form>
      {v.next && (
        <Button.InternalLink to={v.next} variant="secondary">
          Next operations
        </Button.InternalLink>
      )}
    </ManagementLayout>
  )
}
