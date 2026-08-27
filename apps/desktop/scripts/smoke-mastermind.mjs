const [adapter, model, repo] = process.argv.slice(2);

if (!adapter || !model || !repo) {
  console.error("usage: node smoke-mastermind.mjs <adapter> <model> <repo>");
  process.exit(2);
}

const socket = new WebSocket("ws://127.0.0.1:8741");
let requestId = 0;

function call(method, params, timeoutMs = 420_000) {
  return new Promise((resolve, reject) => {
    const id = ++requestId;
    const timer = setTimeout(() => {
      socket.removeEventListener("message", onMessage);
      reject(new Error(`${method} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    function onMessage(event) {
      const frame = JSON.parse(event.data);
      if (frame.id !== id) return;
      clearTimeout(timer);
      socket.removeEventListener("message", onMessage);
      if (frame.ok === false || frame.error) {
        reject(new Error(JSON.stringify(frame.error)));
      } else {
        resolve(frame.result);
      }
    }
    socket.addEventListener("message", onMessage);
    socket.send(JSON.stringify({ id, method, params }));
  });
}

socket.addEventListener("open", async () => {
  try {
    const started = await call("mastermind.start", {
      goal: "Live smoke test: establish discovery for a tiny hello-world application. Do not implement it.",
      repo,
      plannerAdapter: adapter,
      plannerModel: model,
    });
    const turn = await call("mastermind.respond", { sessionId: started.sessionId });
    console.log(JSON.stringify({
      sessionId: started.sessionId,
      adapter: turn.session.plannerAdapter,
      model: turn.session.plannerModel,
      phase: turn.session.phase,
      phaseStatus: turn.session.phaseStatus,
      questions: turn.cycle.decisions?.length || 0,
      modelError: turn.cycle.modelError,
      skillSha256: turn.session.skillSource?.sha256,
      memory: turn.session.memory,
    }, null, 2));
    socket.close();
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    socket.close();
    process.exitCode = 1;
  }
});

socket.addEventListener("error", () => {
  console.error("could not connect to AgentOS daemon");
  process.exitCode = 1;
});
