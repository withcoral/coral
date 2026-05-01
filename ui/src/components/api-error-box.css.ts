import { style } from "@vanilla-extract/css";
import { theme } from "@/wax/theme/theme.css";

export const errorBox = style({
  display: "flex",
  alignItems: "center",
  gap: 8,
  padding: 12,
  margin: 12,
  borderRadius: 6,
  backgroundColor: theme.pill.red.background,
  color: theme.pill.red.color,
});
