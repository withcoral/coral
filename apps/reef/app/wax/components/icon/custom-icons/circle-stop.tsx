import type { LucideProps } from 'lucide-react'

export function CircleStopIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M9 16.5C13.1421 16.5 16.5 13.1421 16.5 9C16.5 4.85786 13.1421 1.5 9 1.5C4.85786 1.5 1.5 4.85786 1.5 9C1.5 13.1421 4.85786 16.5 9 16.5Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
      <path
        d="M10.5 6.75H7.5C7.08579 6.75 6.75 7.08579 6.75 7.5V10.5C6.75 10.9142 7.08579 11.25 7.5 11.25H10.5C10.9142 11.25 11.25 10.9142 11.25 10.5V7.5C11.25 7.08579 10.9142 6.75 10.5 6.75Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
