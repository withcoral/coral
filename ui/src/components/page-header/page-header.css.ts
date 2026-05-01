import { style } from "@vanilla-extract/css";
import { theme } from "@/wax/theme/theme.css";

export const header = style({
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  minHeight: 56,
  paddingInline: 20,
  paddingBlock: 10,
  borderBlockEnd: `1px solid ${theme.stroke.primary}`,
  overflow: "hidden",
});

export const title = style({
  display: "flex",
  alignItems: "center",
  gap: 4,
  flex: 1,
  minWidth: 0,
  paddingInlineEnd: 24,
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
});

export const actions = style({
  display: "flex",
  alignItems: "center",
  gap: 16,
  flexShrink: 0,
});

export const buttonGroup = style({
  display: "flex",
  alignItems: "center",
  gap: 4,
});
