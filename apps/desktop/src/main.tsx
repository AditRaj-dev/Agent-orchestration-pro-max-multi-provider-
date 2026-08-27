import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import { ArtifactPreview } from "./components/ArtifactPreview";

// A popped-out deliverable preview is the same bundle with a query string, so
// the OS window costs one branch instead of a second entrypoint.
const params = new URLSearchParams(window.location.search);
const previewPath = params.get("preview");
const previewSession = params.get("session");

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    {previewPath && previewSession ? (
      <ArtifactPreview sessionId={previewSession} path={previewPath} standalone />
    ) : (
      <App />
    )}
  </React.StrictMode>,
);
