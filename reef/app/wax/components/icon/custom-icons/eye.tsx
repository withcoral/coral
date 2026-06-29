import type { LucideProps } from 'lucide-react'

export function EyeIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M1.54651 9.26099C1.48401 9.09261 1.48401 8.90738 1.54651 8.739C2.15529 7.26289 3.18865 6.00078 4.51559 5.11267C5.84253 4.22456 7.4033 3.75046 9.00001 3.75046C10.5967 3.75046 12.1575 4.22456 13.4844 5.11267C14.8114 6.00078 15.8447 7.26289 16.4535 8.739C16.516 8.90738 16.516 9.09261 16.4535 9.26099C15.8447 10.7371 14.8114 11.9992 13.4844 12.8873C12.1575 13.7754 10.5967 14.2495 9.00001 14.2495C7.4033 14.2495 5.84253 13.7754 4.51559 12.8873C3.18865 11.9992 2.15529 10.7371 1.54651 9.26099Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
      <path
        d="M9.00001 11.25C10.2427 11.25 11.25 10.2426 11.25 9C11.25 7.75735 10.2427 6.75 9.00001 6.75C7.75737 6.75 6.75001 7.75735 6.75001 9C6.75001 10.2426 7.75737 11.25 9.00001 11.25Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
