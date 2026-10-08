import type { LucideProps } from 'lucide-react'

export function RefreshCwIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M2.25 9C2.25 7.21 2.961 5.493 4.227 4.227C5.493 2.961 7.21 2.25 9 2.25C10.887 2.257 12.698 2.993 14.055 4.305L15.75 6M15.75 6V2.25M15.75 6H12M15.75 9C15.75 10.79 15.039 12.507 13.773 13.773C12.507 15.039 10.79 15.75 9 15.75C7.113 15.743 5.302 15.007 3.945 13.695L2.25 12M2.25 12H6M2.25 12V15.75"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
