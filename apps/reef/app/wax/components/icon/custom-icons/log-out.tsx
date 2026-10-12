import type { LucideProps } from 'lucide-react'

export function LogOutIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M12 12.75L15.75 9M15.75 9L12 5.25M15.75 9H6.75M6.75 15.75H3.75C3.352 15.75 2.971 15.592 2.689 15.311C2.408 15.029 2.25 14.648 2.25 14.25V3.75C2.25 3.352 2.408 2.971 2.689 2.689C2.971 2.408 3.352 2.25 3.75 2.25H6.75"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
