import { style, keyframes } from "@vanilla-extract/css";
import { theme } from "@/wax/theme/theme.css";
import { lightTheme } from "@/wax/theme/theme-light.css";

export const root = style({
  display: "flex",
  flexDirection: "column",
  height: "100%",
});

export const body = style({
  display: "flex",
  flex: 1,
  minHeight: 0,
});

export const treePanel = style({
  width: 384,
  flexShrink: 0,
  minHeight: 0,
  display: "flex",
  flexDirection: "column",
  borderInlineEnd: `1px solid ${theme.stroke.primary}`,
});

export const treePanelToolbar = style({
  paddingInline: 12,
  paddingBlock: 10,
  borderBlockEnd: `1px solid ${theme.stroke.primary}`,
});

export const searchRow = style({ position: "relative" });

export const clearButton = style({
  position: "absolute",
  insetInlineEnd: 8,
  top: "50%",
  transform: "translateY(-50%)",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  background: "none",
  border: "none",
  padding: 0,
  cursor: "pointer",
  color: theme.content.tertiary,
  transition: "color 0.15s ease",
  selectors: { "&:hover": { color: theme.content.primary } },
});

export const treeContent = style({ flex: 1, minHeight: 0 });
export const treeList = style({ padding: 4, paddingInlineEnd: 6 });
export const treeEmpty = style({ padding: 12, textAlign: "center" });
export const skeletonContainer = style({ padding: 12, display: "flex", flexDirection: "column", gap: 12 });
export const skeletonGroup = style({ display: "flex", flexDirection: "column", gap: 8 });
export const skeletonChildren = style({ marginInlineStart: 16, display: "flex", flexDirection: "column", gap: 6 });
export const errorContainer = style({ padding: 12, display: "flex", flexDirection: "column", gap: 8 });
export const errorText = style({ fontSize: 13, lineHeight: "20px", color: theme.content.error });

export const connectorButton = style({
  display: "flex",
  alignItems: "center",
  gap: 6,
  width: "100%",
  borderRadius: 4,
  paddingInline: 8,
  paddingBlock: 4,
  background: "none",
  border: "none",
  cursor: "pointer",
  transition: "background-color 0.15s ease",
  selectors: { "&:hover": { backgroundColor: theme.surface.onMainContentSubtle } },
});

export const connectorTableCount = style({ color: theme.content.tertiary, marginInlineStart: "auto" });
export const connectorChildren = style({ marginInlineStart: 16 });
export const tableButton = style({
  display: "flex",
  alignItems: "center",
  gap: 6,
  width: "100%",
  minWidth: 0,
  borderRadius: 4,
  paddingInline: 8,
  paddingBlock: 3,
  background: "none",
  border: "none",
  cursor: "pointer",
  color: theme.content.primary,
  transition: "background-color 0.15s ease",
  selectors: { "&:hover": { backgroundColor: theme.surface.onMainContentSubtle } },
});
export const tableButtonSelected = style({ backgroundColor: theme.surface.onMainContent });
export const tableName = style({ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", minWidth: 0 });
export const columnChildren = style({ marginInlineStart: 24 });
export const columnRow = style({ display: "flex", alignItems: "center", gap: 6, paddingInline: 8, paddingBlock: 2, minWidth: 0 });
export const columnName = style({ display: "block", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", minWidth: 0 });
export const columnType = style({ color: theme.content.secondary });

export const detailPanel = style({ flex: 1, overflow: "auto" });
export const detailContent = style({ padding: 16, display: "flex", flexDirection: "column", gap: 16 });
export const detailEmpty = style({ display: "flex", flexDirection: "column", height: "100%", fontSize: 14, lineHeight: "20px", color: theme.content.secondary });
export const detailEmptyBanner = style({ alignSelf: "stretch" });
export const detailEmptyCenter = style({ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" });
export const description = style({ fontSize: 14, lineHeight: "20px", color: theme.content.secondary, marginBlockStart: 4 });
export const virtualRow = style({ fontStyle: "italic" });
export const cellTruncate = style({ maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" });
export const requiredStar = style({ color: theme.content.error, marginInlineStart: 4 });
export const section = style({ display: "flex", flexDirection: "column", gap: 8 });
export const codeBlock = style({ fontFamily: "'Gustan Mono', monospace", fontSize: 13, lineHeight: "20px", paddingInline: 12, paddingBlock: 8, borderRadius: 6, border: `1px solid ${theme.stroke.primary}`, backgroundColor: theme.surface.mainContent, whiteSpace: "pre", overflowX: "auto", margin: 0 });
export const queryList = style({ display: "flex", flexDirection: "column", gap: 4 });
export const queryEditor = style({
  borderRadius: 6,
  border: `1px solid ${theme.stroke.primary}`,
  backgroundColor: theme.surface.mainContent,
  width: "100%",
  minHeight: 80,
  resize: "vertical",
  overflow: "hidden",
  vars: {
    "--shiki-foreground": "#d4d4d4",
    "--shiki-background": "transparent",
    "--shiki-token-keyword": "#569CD6",
    "--shiki-token-string": "#CE9178",
    "--shiki-token-string-expression": "#CE9178",
    "--shiki-token-comment": "#6A9955",
    "--shiki-token-function": "#4EC9B0",
    "--shiki-token-constant": "#CE9178",
    "--shiki-token-punctuation": "#d4d4d4",
    "--shiki-token-parameter": "#9CDCFE",
    "--shiki-token-link": "#82aaff",
  },
  selectors: {
    "&:focus-within": { borderColor: theme.stroke.focused },
    [`.${lightTheme} &`]: {
      vars: {
        "--shiki-foreground": "#1e1e1e",
        "--shiki-background": "transparent",
        "--shiki-token-keyword": "#0000FF",
        "--shiki-token-string": "#A31515",
        "--shiki-token-string-expression": "#A31515",
        "--shiki-token-comment": "#008000",
        "--shiki-token-function": "#795E26",
        "--shiki-token-constant": "#098658",
        "--shiki-token-punctuation": "#1e1e1e",
        "--shiki-token-parameter": "#001080",
        "--shiki-token-link": "#0000FF",
      },
    },
  },
});
export const queryActions = style({ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" });
export const sampleDataHeader = style({ display: "flex", alignItems: "center", gap: 8 });
export const sampleDataError = style({ borderRadius: 6, border: `1px solid ${theme.pill.red.stroke}`, backgroundColor: theme.pill.red.background, paddingInline: 12, paddingBlock: 8, fontSize: 13, lineHeight: "20px", color: theme.pill.red.color });
export const preBlock = style({ fontFamily: "'Gustan Mono', monospace", fontSize: 13, lineHeight: "20px", padding: 12, borderRadius: 6, border: `1px solid ${theme.stroke.primary}`, backgroundColor: theme.surface.mainContent, whiteSpace: "pre", overflowX: "auto", margin: 0 });
const spin = keyframes({ from: { transform: "rotate(0deg)" }, to: { transform: "rotate(360deg)" } });
export const spinner = style({ animationName: spin, animationDuration: "1s", animationIterationCount: "infinite", animationTimingFunction: "linear" });
