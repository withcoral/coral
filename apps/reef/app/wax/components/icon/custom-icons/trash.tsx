import type { LucideProps } from 'lucide-react'

export function TrashIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M14.25 4.5V15C14.25 15.398 14.092 15.779 13.811 16.061C13.529 16.342 13.148 16.5 12.75 16.5H5.25C4.852 16.5 4.471 16.342 4.189 16.061C3.908 15.779 3.75 15.398 3.75 15V4.5M2.25 4.5H15.75M6 4.5V3C6 2.602 6.158 2.221 6.439 1.939C6.721 1.658 7.102 1.5 7.5 1.5H10.5C10.898 1.5 11.279 1.658 11.561 1.939C11.842 2.221 12 2.602 12 3V4.5"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
