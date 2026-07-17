import type { LucideProps } from 'lucide-react'

export function NotepadTextIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
  return (
    <svg
      fill="none"
      height={size}
      viewBox="0 0 18 18"
      width={size}
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <path
        d="M6 1.5V4.5M9 1.5V4.5M12 1.5V4.5M6 7.5H10.5M6 10.5H12M6 13.5H9.75M4.5 3H13.5C14.328 3 15 3.672 15 4.5V15C15 15.828 14.328 16.5 13.5 16.5H4.5C3.672 16.5 3 15.828 3 15V4.5C3 3.672 3.672 3 4.5 3Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
