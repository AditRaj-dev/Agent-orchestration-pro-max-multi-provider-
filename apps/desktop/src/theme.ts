export type ThemeName = "light" | "dark";

/**
 * Apply a theme to the document.
 *
 * Chromium keeps the previously interpolated value of a running colour
 * transition when the custom property driving it changes, so flipping
 * `data-theme` on its own leaves already-painted elements on the old palette.
 * Suppressing transitions for the swap frame — and only that frame — is what
 * makes the new tokens actually reach every element.
 *
 * Both the header toggle and the Settings appearance control route through
 * here so the two cannot drift apart.
 */
export function applyTheme(theme: ThemeName): () => void {
  const root = document.documentElement;
  root.classList.add("theme-swapping");
  root.setAttribute("data-theme", theme);
  root.classList.toggle("theme-dark", theme === "dark");
  root.classList.toggle("theme-light", theme !== "dark");
  document.body.className = theme === "dark" ? "theme-dark" : "theme-light";

  const frame = window.requestAnimationFrame(() => {
    window.requestAnimationFrame(() => root.classList.remove("theme-swapping"));
  });
  return () => window.cancelAnimationFrame(frame);
}

/** The theme the OS is currently asking for. */
export function systemTheme(): ThemeName {
  return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
}
