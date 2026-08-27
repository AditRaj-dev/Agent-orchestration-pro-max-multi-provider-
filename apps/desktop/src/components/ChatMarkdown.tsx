// Renders one provider turn's prose inside the Mastermind transcript.
//
// The text is model output, so it is untrusted at this boundary: it is parsed
// into markup and then stripped of anything executable by the same sanitizer
// the artifact viewer uses before React ever sees it.
import { useMemo } from "react";
import { marked } from "marked";
import { sanitizeMarkup } from "./ArtifactPreview";

export function ChatMarkdown({ text }: { text: string }) {
  const html = useMemo(
    () => sanitizeMarkup(marked.parse(text, { async: false, gfm: true, breaks: true }) as string),
    [text],
  );
  return <div className="chat-markdown" dangerouslySetInnerHTML={{ __html: html }} />;
}
