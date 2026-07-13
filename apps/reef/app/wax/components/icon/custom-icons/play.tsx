import type { LucideProps } from 'lucide-react'

export function PlayIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M2.999 3.75C2.999 3.486 3.069 3.227 3.201 2.998C3.333 2.77 3.523 2.58 3.752 2.449C3.981 2.317 4.241 2.249 4.505 2.25C4.768 2.25 5.027 2.321 5.255 2.454L14.253 7.702C14.48 7.834 14.669 8.023 14.8 8.251C14.931 8.478 15 8.736 15 8.999C15.001 9.261 14.932 9.519 14.801 9.747C14.67 9.974 14.482 10.164 14.255 10.296L5.255 15.546C5.027 15.679 4.768 15.75 4.505 15.75C4.241 15.751 3.981 15.683 3.752 15.551C3.523 15.42 3.333 15.23 3.201 15.002C3.069 14.773 2.999 14.514 2.999 14.25V3.75Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
