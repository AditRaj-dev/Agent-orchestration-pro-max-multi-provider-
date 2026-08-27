// @vitest-environment jsdom
import { describe, expect, it } from "vitest";
import { marked } from "marked";
import { sanitizeMarkup } from "./ArtifactPreview";

describe("deliverable preview sanitizer", () => {
  it("keeps the prose, tables and code a Mastermind document is made of", () => {
    const html = sanitizeMarkup(
      marked.parse(
        "# Discovery\n\nA sentence with `code`.\n\n| Layer | Choice |\n|---|---|\n| Frontend | Next.js |\n\n- one\n- two\n",
        { async: false, gfm: true },
      ) as string,
    );

    expect(html).toContain("<h1>Discovery</h1>");
    expect(html).toContain("<code>code</code>");
    expect(html).toContain("<table>");
    expect(html).toContain("Next.js");
    expect(html).toContain("<li>one</li>");
  });

  it("drops executable elements an authored document has no reason to contain", () => {
    const html = sanitizeMarkup(
      '<p>before</p><script>window.stolen = 1</script><iframe src="http://evil"></iframe><p>after</p>',
    );

    expect(html).toContain("before");
    expect(html).toContain("after");
    expect(html).not.toContain("<script");
    expect(html).not.toContain("<iframe");
  });

  it("strips event handlers and script URLs while leaving the element itself", () => {
    const html = sanitizeMarkup(
      '<img src="x" onerror="window.stolen = 1"><a href="javascript:alert(1)">click</a>' +
        '<a href="https://example.com">safe</a>',
    );

    expect(html).not.toContain("onerror");
    expect(html).not.toContain("javascript:");
    expect(html).toContain("click");
    expect(html).toContain('href="https://example.com"');
  });

  it("is not fooled by whitespace or casing inside a script URL", () => {
    const html = sanitizeMarkup('<a href="Java\nscript: alert(1)">x</a><a href="JAVASCRIPT:alert(1)">y</a>');

    expect(html.toLowerCase()).not.toContain("script:");
  });
});
