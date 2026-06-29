import type { LucideProps } from 'lucide-react'

export function LockIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M5.25 8.25V5.25C5.25 4.255 5.645 3.302 6.348 2.598C7.052 1.895 8.005 1.5 9 1.5C9.995 1.5 10.948 1.895 11.652 2.598C12.355 3.302 12.75 4.255 12.75 5.25V8.25M3.75 8.25H14.25C15.078 8.25 15.75 8.922 15.75 9.75V15C15.75 15.828 15.078 16.5 14.25 16.5H3.75C2.922 16.5 2.25 15.828 2.25 15V9.75C2.25 8.922 2.922 8.25 3.75 8.25Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
