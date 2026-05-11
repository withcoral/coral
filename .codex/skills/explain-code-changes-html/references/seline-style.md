# Seline Analytics Style Reference

Use this reference when styling the standalone HTML explainer and the user has
not supplied a different design system. Treat it as a functional analytics
document style, not a marketing landing page.

## Visual Direction

- Theme: light.
- Mood: crisp, lightweight, airy, and utilitarian.
- Base: monochromatic neutrals with one vivid blue accent.
- Layout: centered max-width content on a warm off-white page background, with
  compact cards, subtle borders, and soft shadows.
- Visuals: use diagrams, tables, screenshots, and minimal outlined icons to
  explain the change. Avoid decorative imagery that does not clarify the code.

## Color Tokens

```css
:root {
  --cloud-white: #ffffff;
  --canvas-fog: #fafaf9;
  --slate-text: #0c0a09;
  --ash-gray: #78716c;
  --stone-border: #e5e7eb;
  --platinum-outline: #d6d3d1;
  --steel-gray: #a8a29e;
  --hover-stone: #c9c5c2;
  --ghost-ink: #1c1917;
  --chartwell-blue: #3ba6f1;
  --sky-tint: #c1e1f7;
}
```

- Use `--canvas-fog` for the page background.
- Use `--cloud-white` for cards, tables, controls, and document surfaces.
- Use `--slate-text` for headings and primary copy.
- Use `--ash-gray` for secondary text, helper text, and inactive icons.
- Use `--steel-gray` for tertiary labels and low-priority details.
- Use `--stone-border` and `--platinum-outline` for dividers and inputs.
- Use `--chartwell-blue` only for primary actions, active states, links,
  important data points, and brand accents.
- Use `--sky-tint` sparingly for cool callouts or highlighted document bands.

## Typography

- Prefer Inter for body text, UI labels, captions, navigation, tables, and
  descriptions. Fall back to `system-ui`.
- Prefer roobert for headings and prominent display text when available. Fall
  back to `sans-serif`.
- Use practical sizes: `12px`, `13px`, `14px`, `15px`, `16px`, `18px`, `20px`,
  `32px`, and only use `52px` for a true first-screen title.
- Keep body line-height near `1.5` and headings near `1.2` to `1.25`.
- Keep type lightweight: Inter `400`, `500`, `600`; roobert `400`, `500`.
- Avoid negative letter spacing in generated explainers unless the target repo
  already uses exact Seline brand typography tokens.

## Spacing and Shape

- Base spacing unit: `4px`.
- Default section gap: `48px`.
- Default card padding: `24px`.
- Default element gap: `8px`.
- Cards: `10px` radius.
- Feature cards: `16px` radius.
- Inputs: `4px` radius, or `6px` for rounded search/filter fields.
- Tags, pills, and primary buttons: `9999px` radius.

## Elevation

```css
:root {
  --shadow-card: rgba(0, 0, 0, 0.05) 0 4px 16px 0;
  --shadow-control: rgba(0, 0, 0, 0.05) 0 1px 2px 0;
  --shadow-feature: rgba(17, 12, 46, 0.12) 0 12px 45px 0;
  --shadow-icon: rgba(0, 0, 0, 0.1) 0 4px 6px -1px,
    rgba(0, 0, 0, 0.1) 0 2px 4px -2px;
}
```

- Use `--shadow-card` for dashboard cards and pill cards.
- Use `--shadow-control` for compact navigation and buttons.
- Reserve `--shadow-feature` for one or two prominent summary or hero panels.
- Avoid heavy dark shadows.

## Components

- Primary filled button: `--chartwell-blue` background, white text, compact
  pill shape.
- Light ghost button: transparent background, `--ash-gray` text, optional
  `--stone-border` border.
- Dark ghost button: transparent background, `--slate-text` text,
  `--stone-border` border, `4px` radius.
- Subtle ghost button: lightly tinted gray background, `--slate-text` text,
  `--stone-border` border, `4px` radius.
- Dashboard card: white background, `10px` radius, `24px` padding,
  `--shadow-card`.
- Pill card: white background, pill radius, `4px 12px` padding,
  `--shadow-card`.
- Elevated feature card: white background, `16px` radius, compact inner padding,
  `--shadow-feature`.
- Standard input: white background, `--slate-text` text,
  `--platinum-outline` border, square or `4px` radius.
- Rounded search/filter input: white background, `--ash-gray` text,
  `--platinum-outline` border, `6px` radius, `4px 12px` padding.

## HTML Explainer Application

- Build the first screen as a focused document header, not a product hero.
- Put the executive summary in compact cards near the top.
- Use full-width document sections or constrained content bands; avoid nested
  cards.
- Use tables for code maps, contract fields, and test matrices.
- Use inline SVG or Mermaid diagrams inside white cards with captions.
- Use blue for active states and key data only; do not add extra saturated
  accent colors.
- Keep controls and filters compact, with predictable placement.
- Keep mobile layout single-column with readable tables, cards, and diagrams.
- Preserve accessibility: high contrast, labels beyond color, no tiny diagram
  text, and no text overlap.
