import type { LucideProps } from 'lucide-react'

export function HomeIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M11.25 16.125V10.125C11.25 9.926 11.171 9.735 11.03 9.595C10.89 9.454 10.699 9.375 10.5 9.375H7.5C7.301 9.375 7.11 9.454 6.97 9.595C6.829 9.735 6.75 9.926 6.75 10.125V16.125M2.25 7.875C2.25 7.657 2.297 7.441 2.389 7.243C2.481 7.046 2.615 6.87 2.782 6.729L8.032 2.229C8.302 2 8.645 1.875 9 1.875C9.354 1.875 9.697 2 9.968 2.229L15.218 6.729C15.385 6.87 15.519 7.046 15.611 7.243C15.702 7.441 15.75 7.657 15.75 7.875V14.625C15.75 15.023 15.592 15.404 15.311 15.686C15.029 15.967 14.648 16.125 14.25 16.125H3.75C3.352 16.125 2.971 15.967 2.689 15.686C2.408 15.404 2.25 15.023 2.25 14.625V7.875Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
