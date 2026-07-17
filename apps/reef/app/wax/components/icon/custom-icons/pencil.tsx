import type { LucideProps } from 'lucide-react'

export function PencilIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M11.253 3.748L14.253 6.748M15.883 5.107C16.28 4.71 16.502 4.173 16.503 3.612C16.503 3.051 16.28 2.514 15.883 2.117C15.487 1.721 14.949 1.498 14.389 1.498C13.828 1.498 13.29 1.72 12.894 2.117L2.884 12.128C2.71 12.302 2.581 12.516 2.509 12.751L1.518 16.015C1.499 16.08 1.497 16.149 1.514 16.214C1.531 16.28 1.565 16.34 1.613 16.388C1.661 16.436 1.721 16.469 1.786 16.486C1.852 16.502 1.921 16.501 1.986 16.481L5.25 15.491C5.485 15.42 5.699 15.292 5.873 15.119L15.883 5.107Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
