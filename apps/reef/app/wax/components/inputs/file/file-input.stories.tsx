import type { Meta, StoryObj } from '@storybook/react-vite'
import { useRef, useState } from 'react'
import { expect, fireEvent, fn, userEvent, waitFor } from 'storybook/test'

import { FileInput } from './file-input'

const dropLabel = 'Drop a JSON or YAML file here.'
const firstFile = () => new File(['{"first":true}\n'], 'first.json', { type: 'application/json' })
const replacementFile = () =>
  new File(['replacement: true\n'], 'replacement.yaml', { type: 'application/yaml' })

function picker(canvasElement: HTMLElement) {
  return canvasElement.querySelector<HTMLInputElement>('input[type="file"]')!
}

async function drop(target: HTMLElement, ...files: File[]) {
  const dataTransfer = new DataTransfer()
  for (const file of files) dataTransfer.items.add(file)
  // fireEvent.drop shallow-copies DataTransfer and loses its native FileList.
  await fireEvent(target, new DragEvent('drop', { bubbles: true, cancelable: true, dataTransfer }))
}

async function selectFile(input: HTMLInputElement, file: File) {
  // userEvent.upload mocks files; native FormData and reset need a real FileList.
  const transfer = new DataTransfer()
  transfer.items.add(file)
  input.files = transfer.files
  await fireEvent.input(input)
  await fireEvent.change(input)
}

async function expectFormFile(input: HTMLInputElement, file: File) {
  const submitted = new FormData(input.form!).get('document') as File
  await expect(input.files).toHaveLength(1)
  await expect(submitted.name).toBe(file.name)
  await expect(submitted.type).toBe(file.type)
  await expect(submitted.size).toBe(file.size)
  await expect(await submitted.text()).toBe(await file.text())
}

const meta = {
  component: FileInput,
  title: 'Components/Inputs/FileInput',
  args: {
    ariaLabel: 'Document file',
    accept: '.json,.yaml,.yml',
    name: 'document',
    onChange: fn(),
  },
  decorators: [
    (Story) => (
      <form onSubmit={(event) => event.preventDefault()}>
        <Story />
      </form>
    ),
  ],
} satisfies Meta<typeof FileInput>
export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  play: async ({ canvasElement }) => {
    const input = picker(canvasElement)
    const file = firstFile()
    await selectFile(input, file)
    await expectFormFile(input, file)
  },
}
export const Invalid: Story = {
  args: { invalid: true },
  play: async ({ canvasElement }) => {
    await expect(picker(canvasElement)).toHaveAttribute('aria-invalid', 'true')
    await expect(picker(canvasElement)).toHaveAttribute('data-invalid')
  },
}
export const Disabled: Story = {
  args: { disabled: true },
  play: async ({ canvasElement }) => {
    await expect(picker(canvasElement)).toBeDisabled()
  },
}

export const DropArea: Story = { args: { dropLabel } }
export const Dragging: Story = {
  args: { dropLabel },
  play: async ({ canvas }) => {
    const dataTransfer = new DataTransfer()
    dataTransfer.items.add(firstFile())
    const instructions = canvas.getByText(dropLabel)
    await fireEvent(
      instructions,
      new DragEvent('dragenter', { bubbles: true, cancelable: true, dataTransfer }),
    )
    // Moving over a child must not remove the outer drag treatment.
    await fireEvent(
      canvas.getByRole('button'),
      new DragEvent('dragenter', { bubbles: true, cancelable: true, dataTransfer }),
    )
    await fireEvent(
      canvas.getByRole('button'),
      new DragEvent('dragleave', { bubbles: true, cancelable: true, dataTransfer }),
    )
    await waitFor(() => expect(instructions.parentElement).toHaveAttribute('data-dropping'))
  },
}
export const Selected: Story = {
  args: { dropLabel },
  play: async ({ args, canvas, canvasElement }) => {
    const input = picker(canvasElement)
    const first = firstFile()
    const replacement = replacementFile()
    await drop(canvas.getByText(dropLabel), first, replacement)
    await expectFormFile(input, first)
    await expect(args.onChange).toHaveBeenLastCalledWith(first)
    await waitFor(() => expect(canvas.getByRole('status')).toHaveTextContent(first.name))

    // Opening and canceling the picker retains the dropped selection.
    const clicked = fn((event: Event) => event.preventDefault())
    input.addEventListener('click', clicked)
    await userEvent.click(canvas.getByRole('button', { name: 'Choose a file' }))
    input.removeEventListener('click', clicked)
    await expect(clicked).toHaveBeenCalledOnce()
    await fireEvent(input, new Event('cancel', { bubbles: true }))
    await expectFormFile(input, first)
    await expect(args.onChange).toHaveBeenCalledTimes(1)

    await selectFile(input, replacement)
    await expectFormFile(input, replacement)
    await waitFor(() => expect(canvas.getByRole('status')).toHaveTextContent(replacement.name))
    const text = new DataTransfer()
    text.setData('text/plain', 'No file')
    await expect(
      await fireEvent(
        canvas.getByText(dropLabel),
        new DragEvent('drop', {
          bubbles: true,
          cancelable: true,
          dataTransfer: text,
        }),
      ),
    ).toBe(false)
    await drop(canvas.getByText(dropLabel))
    await expectFormFile(input, replacement)
    await expect(args.onChange).toHaveBeenCalledTimes(2)
  },
}
export const DropDisabled: Story = {
  args: { dropLabel, disabled: true },
  play: async ({ args, canvas, canvasElement }) => {
    const input = picker(canvasElement)
    await drop(canvas.getByText(dropLabel), firstFile())
    await expect(input).toBeDisabled()
    await expect(canvas.getByRole('button')).toBeDisabled()
    await expect(input.files).toHaveLength(0)
    await expect(new FormData(input.form!).has('document')).toBe(false)
    await expect(args.onChange).not.toHaveBeenCalled()
  },
}
export const DropInvalid: Story = {
  args: { dropLabel, invalid: true },
  play: async ({ canvas, canvasElement }) => {
    const file = firstFile()
    await drop(canvas.getByText(dropLabel), file)
    await expectFormFile(picker(canvasElement), file)
    await expect(picker(canvasElement)).toHaveAttribute('aria-invalid', 'true')
  },
}
export const Reset: Story = {
  args: { dropLabel },
  render: (args) => (
    <>
      <FileInput {...args} />
      <button type="reset">Reset</button>
    </>
  ),
  play: async ({ args, canvas, canvasElement }) => {
    const input = picker(canvasElement)
    const first = firstFile()
    await drop(canvas.getByText(dropLabel), first)
    const cancelReset = (event: Event) => event.preventDefault()
    input.form!.addEventListener('reset', cancelReset)
    await userEvent.click(canvas.getByRole('button', { name: 'Reset' }))
    input.form!.removeEventListener('reset', cancelReset)
    await expectFormFile(input, first)
    await expect(args.onChange).toHaveBeenCalledTimes(1)

    await userEvent.click(canvas.getByRole('button', { name: 'Reset' }))
    await waitFor(() => expect(args.onChange).toHaveBeenLastCalledWith(null))
    await expect(input.files).toHaveLength(0)
    await expect((new FormData(input.form!).get('document') as File).size).toBe(0)
    await waitFor(() => expect(canvas.getByRole('status', { hidden: true })).toBeEmptyDOMElement())
    await selectFile(input, replacementFile())
    await expectFormFile(input, replacementFile())
  },
}
export const DisabledSelection: Story = {
  args: { dropLabel },
  render: (args) => {
    const [disabled, setDisabled] = useState(false)
    return (
      <>
        <FileInput {...args} disabled={disabled} />
        <button type="button" onClick={() => setDisabled(!disabled)}>
          Toggle disabled
        </button>
      </>
    )
  },
  play: async ({ canvas, canvasElement }) => {
    const input = picker(canvasElement)
    const file = firstFile()
    await drop(canvas.getByText(dropLabel), file)
    await userEvent.click(canvas.getByRole('button', { name: 'Toggle disabled' }))
    await drop(canvas.getByText(dropLabel), replacementFile())
    await expect(input.files?.[0].name).toBe(file.name)
    await expect(canvas.getByRole('status')).toHaveTextContent(file.name)
    await userEvent.click(canvas.getByRole('button', { name: 'Toggle disabled' }))
    await expectFormFile(input, file)
  },
}
export const NativeRef: Story = {
  args: { dropLabel, id: 'document-picker', required: true },
  render: (args) => {
    const ref = useRef<HTMLInputElement>(null)
    return (
      <>
        <FileInput {...args} ref={ref} />
        <button type="button" onClick={() => ref.current?.focus()}>
          Focus input
        </button>
      </>
    )
  },
  play: async ({ canvas, canvasElement }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Focus input' }))
    const input = picker(canvasElement)
    await expect(input).toHaveFocus()
    await expect(input).toHaveAttribute('id', 'document-picker')
    await expect(input).toBeRequired()
    await expect(input).toBeInvalid()
  },
}
