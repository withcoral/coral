import type { LucideProps } from 'lucide-react'

export function DocIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M9 5.25V15.75M9 5.25C9 4.454 8.684 3.691 8.121 3.129C7.559 2.566 6.796 2.25 6 2.25H2.25C2.051 2.25 1.86 2.329 1.72 2.47C1.579 2.61 1.5 2.801 1.5 3V12.75C1.5 12.949 1.579 13.14 1.72 13.28C1.86 13.421 2.051 13.5 2.25 13.5H6.75C7.347 13.5 7.919 13.737 8.341 14.159C8.763 14.581 9 15.153 9 15.75M9 5.25C9 4.454 9.316 3.691 9.879 3.129C10.441 2.566 11.204 2.25 12 2.25H15.75C15.949 2.25 16.14 2.329 16.28 2.47C16.421 2.61 16.5 2.801 16.5 3V12.75C16.5 12.949 16.421 13.14 16.28 13.28C16.14 13.421 15.949 13.5 15.75 13.5H11.25C10.653 13.5 10.081 13.737 9.659 14.159C9.237 14.581 9 15.153 9 15.75"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
