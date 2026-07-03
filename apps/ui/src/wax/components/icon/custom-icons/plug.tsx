import type { LucideProps } from 'lucide-react'

export function PlugIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
  return (
    <svg
      fill="none"
      height={size}
      stroke={color}
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth="1.2525"
      viewBox="0 0 11 17"
      width={size}
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <path d="M5.12622 15.6262V11.8762M7.37622 5.12622V0.626221M2.87622 5.12622V0.626221M8.87622 5.12622C9.07513 5.12622 9.2659 5.20524 9.40655 5.34589C9.5472 5.48654 9.62622 5.67731 9.62622 5.87622V8.87622C9.62622 9.67187 9.31015 10.4349 8.74754 10.9975C8.18493 11.5602 7.42187 11.8762 6.62622 11.8762H3.62622C2.83057 11.8762 2.06751 11.5602 1.5049 10.9975C0.942291 10.4349 0.626221 9.67187 0.626221 8.87622V5.87622C0.626221 5.67731 0.705238 5.48654 0.845891 5.34589C0.986543 5.20524 1.17731 5.12622 1.37622 5.12622H8.87622Z" />
    </svg>
  )
}
