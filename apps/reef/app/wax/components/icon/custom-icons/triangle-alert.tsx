import type { LucideProps } from 'lucide-react'

export function TriangleAlertIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M9.008 6.754V9.754M9.008 12.754H9.015M16.305 13.504L10.305 3.004C10.174 2.773 9.985 2.581 9.755 2.448C9.526 2.314 9.266 2.244 9 2.244C8.735 2.244 8.474 2.314 8.245 2.448C8.016 2.581 7.826 2.773 7.695 3.004L1.695 13.504C1.563 13.733 1.494 13.993 1.494 14.257C1.495 14.522 1.565 14.782 1.699 15.01C1.832 15.238 2.023 15.427 2.253 15.558C2.483 15.689 2.743 15.756 3.008 15.754H15.008C15.271 15.754 15.529 15.684 15.757 15.552C15.985 15.421 16.174 15.231 16.306 15.003C16.437 14.775 16.506 14.517 16.506 14.254C16.506 13.991 16.437 13.732 16.305 13.504Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
