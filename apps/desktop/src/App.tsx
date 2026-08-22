// F-11b placeholder — the desktop build agent replaces this with the
// command-center shell (docs/F-11-desktop.md §4). The app is a pure client
// of the daemon WS API; default endpoint ws://127.0.0.1:8741, overridable
// via AGENTOS_WS_ADDR / ?ws= query / Settings.
export default function App() {
  return (
    <main style={{ padding: 24, fontFamily: "ui-monospace, monospace" }}>
      Agent Engineering OS — desktop shell not wired yet (F-11b).
      Daemon endpoint: ws://127.0.0.1:8741
    </main>
  );
}
