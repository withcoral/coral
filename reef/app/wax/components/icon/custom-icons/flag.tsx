import type { LucideProps } from 'lucide-react'

export function FlagIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M3 16.5V3C3 2.884 3.027 2.769 3.079 2.665C3.131 2.56 3.207 2.47 3.3 2.4C4.079 1.816 5.026 1.5 6 1.5C8.25 1.5 9.75 3 11.5 3C12.5 3 13.266 2.8 13.8 2.4C13.911 2.316 14.044 2.266 14.183 2.253C14.321 2.241 14.461 2.267 14.585 2.329C14.71 2.391 14.815 2.487 14.888 2.606C14.961 2.724 15 2.861 15 3V10.5C15 10.616 14.973 10.731 14.921 10.835C14.869 10.94 14.793 11.03 14.7 11.1C13.921 11.684 12.974 12 12 12C9.75 12 8.25 10.5 6 10.5C4.893 10.5 3.825 10.908 3 11.646"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
