# Design System: Agent Engineering OS

## 1. Visual Theme & Atmosphere

A staff engineer works in a dim studio with several live agents across two monitors, scanning intent, authority, and failure without visual fatigue. The interface should feel like calibrated engineering instrumentation: dense enough for serious work, restrained enough to disappear while the user concentrates.

- Density: 7/10 — compact operational information with deliberate breathing room around primary work.
- Variance: 5/10 — asymmetric workspace proportions, conventional controls, no novelty for its own sake.
- Motion: 4/10 — short state transitions plus clearly legible real-event travel on the workflow graph.
- Color strategy: restrained. One moss-green brand family carries selection and primary action; semantic colors are reserved for actual system state.

## 2. Color Palette & Roles

Canonical tokens use OKLCH. Hex exports may be generated for external design tools, but application code must consume the OKLCH tokens.

### Dark theme

- **Night Canvas** — `oklch(0.105 0 0)`: application background.
- **Instrument Surface** — `oklch(0.145 0.008 140)`: navigation, inspector, and composer surfaces.
- **Raised Instrument** — `oklch(0.180 0.010 140)`: menus, selected rows, and elevated controls.
- **Primary Ink** — `oklch(0.940 0.005 140)`: primary text and icons.
- **Muted Ink** — `oklch(0.680 0.012 140)`: secondary labels after contrast verification.
- **Moss Signal** — `oklch(0.720 0.130 140)`: focus rings, selected edges, active indicators, and inline links.
- **Moss Action** — `oklch(0.460 0.120 140)`: filled primary actions with near-white text.

### Light theme

- **Day Canvas** — `oklch(1.000 0 0)`: application background.
- **Day Surface** — `oklch(0.975 0.004 140)`: navigation and inspector surfaces.
- **Day Raised** — `oklch(0.945 0.006 140)`: menus and selected rows.
- **Day Ink** — `oklch(0.190 0.010 140)`: primary text and icons.
- **Day Muted** — `oklch(0.470 0.015 140)`: secondary labels after contrast verification.
- **Deep Moss** — `oklch(0.350 0.110 140)`: primary actions, focus, links, and selection.

Error, warning, information, and success colors are semantic-only. Each state also requires an icon, label, or structural treatment. Do not use semantic colors as decoration or agent identity colors.

## 3. Typography Rules

- **Interface:** Geist Sans, with a high-quality system sans fallback.
- **Technical:** Geist Mono, with a monospace fallback, for paths, providers, models, timestamps, identifiers, diffs, and usage values.
- Use one sans family across headings, controls, and prose. Serif type is not used in the application.
- Fixed scale: 12, 13, 14, 16, 18, 22, and 28px. Use weight and spacing before increasing size.
- Body prose is limited to 65–75 characters per line; transcripts and code artifacts may use wider structured layouts.
- Display tracking never goes below `-0.04em`.

## 4. Component Styling

- **Buttons:** 8px radius, clear default/hover/focus/active/disabled/loading states, and a one-pixel tactile active translation. No outer glow.
- **Inputs:** label above, helper below, inline error below. Permission-bearing inputs include resolved scope feedback.
- **Panels:** structural surfaces separated by tonal contrast or a one-pixel divider. Avoid wrapping every region in a card.
- **Cards:** reserved for independently movable or selectable objects such as project records. Maximum 12px radius; never nest cards.
- **Menus and popovers:** rendered through a portal or native top layer so scroll containers cannot clip them.
- **Loading:** skeletons match final geometry. Compact progress indicators are permitted for actions with known progress; generic central spinners are not.
- **Empty states:** teach the next meaningful action using actual project, agent, or provider state.
- **Messages:** agent turns are document-like and unboxed; user turns use one restrained contrasting surface. Tool events are collapsible timeline rows.
- **Graph nodes:** show status, agent, model, permission, attempts, and blocking reason without requiring hover.

## 5. Layout Principles

- Desktop minimum: 1024 × 700.
- At 1280px and above: persistent project rail, central work surface, and contextual inspector.
- At 1024–1279px: icon rail plus inspector overlay; the central task remains usable without horizontal page scrolling.
- Chat prose stays centered within a readable measure while tool events, diffs, and artifacts may expand to the available work surface.
- Grid is used for the three-region shell; flex layouts handle toolbars and one-dimensional control groups.
- Use a semantic z-index scale: base, sticky, popover, backdrop, modal, toast, tooltip.

## 6. Motion & Interaction

- Standard transitions: 150–220ms using quartic or exponential ease-out.
- Workflow packets: 300–600ms based on edge length. Packets exist only for journaled outputs, handoffs, artifacts, approvals, reviews, and Git events.
- Never animate layout dimensions during routine interaction. Prefer transform and opacity.
- No page-load choreography and no perpetual decorative loops.
- Reduced-motion mode replaces travel and slide transitions with an immediate state change or short crossfade.
- Historical events do not animate after reconnect unless replay mode is explicitly active.

## 7. Anti-Patterns

- No purple/blue neon AI aesthetic, gradient text, glassmorphism, or decorative glow.
- No pure black text/background token, oversized rounded containers, side-stripe callouts, or wide ghost-card shadows.
- No identical three-column card grids, fake metrics, fake activity, emojis as interface icons, or generic filler copy.
- No custom cursors, decorative scroll instructions, hidden hover-only critical state, or motion without semantic meaning.
- No permission-changing shortcut may bypass a confirmation surface.
- No raw provider payload, secret, or unrestricted log may be presented as memory or silently included in a handoff.
