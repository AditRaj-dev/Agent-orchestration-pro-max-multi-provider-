//! Plan-mode decisions: the provider-independent half of
//! [`AdapterEvent::Decision`].
//!
//! Two ways a session asks the human to choose, both landing on the same
//! event so the desktop renders one kind of button row:
//!
//! - **Tool-shaped** ([`from_tool_input`]) — the provider exposes a real
//!   asking tool and hands us its input JSON. Claude Code does this
//!   (`AskUserQuestion`, `ExitPlanMode`, F-03). Matching is by *shape*
//!   first (a `questions` array, a `plan` string, a `question` + `options`
//!   pair), so a provider that names its tool something else still works:
//!   codex (F-04, not yet implemented here) routes its approval prompts
//!   through the same call once its adapter lands.
//! - **Text-shaped** ([`from_text`]) — the provider has no asking tool at
//!   all and the model writes the question into its answer as a fenced
//!   JSON block. This is the agy path (F-05): its stream carries only
//!   `text_delta`/`usage`, no tool events, so the protocol lives in the
//!   text. The `decision-protocol` skill (F-13 seeds) teaches agy agents
//!   the exact block to emit:
//!
//!   ```text
//!   ```json
//!   {"ask": {"question": "Which database?",
//!            "options": ["Postgres", "SQLite"], "multiSelect": false}}
//!   ```
//!   ```
//!
//! Both paths are lossy-tolerant: anything that does not parse is simply
//! not a decision. A malformed block must never fail a session.

use serde_json::Value;

use crate::events::AdapterEvent;

/// Approve/reject options synthesized for a plan submitted without any.
const PLAN_OPTIONS: [&str; 2] = [
    "Approve the plan and start building",
    "Keep planning - I have changes",
];

/// Decisions carried by a tool call's input JSON.
///
/// Recognized shapes, in order: a `questions` array (one decision per
/// entry), a `plan`/`plan_text` string (approve or keep planning), or a
/// bare `question`/`prompt` beside an `options` array. Unknown shapes
/// yield nothing — the ordinary `ToolUse` event already recorded the call.
pub fn from_tool_input(tool: &str, input: &Value) -> Vec<AdapterEvent> {
    if let Some(questions) = input.get("questions").and_then(Value::as_array) {
        return questions
            .iter()
            .filter_map(|question| decision_from_question(tool, question))
            .collect();
    }
    if let Some(plan) = str_field(input, "plan").or_else(|| str_field(input, "plan_text")) {
        return vec![AdapterEvent::Decision {
            tool: tool.to_owned(),
            prompt: plan,
            options: PLAN_OPTIONS.iter().map(|s| (*s).to_owned()).collect(),
            multi_select: false,
        }];
    }
    decision_from_question(tool, input).into_iter().collect()
}

/// Decisions the model wrote into its own text as fenced JSON blocks.
///
/// A block counts when it is (or contains, under `ask`/`decision`) an
/// object carrying a question and a non-empty `options` array. Everything
/// else in the text is left alone — this runs over ordinary answers.
pub fn from_text(text: &str) -> Vec<AdapterEvent> {
    let mut decisions = Vec::new();
    for block in fenced_blocks(text) {
        let Ok(value) = serde_json::from_str::<Value>(&block) else {
            continue;
        };
        for candidate in ask_candidates(&value) {
            if let Some(event) = decision_from_question("ask", &candidate) {
                decisions.push(event);
            }
        }
    }
    decisions
}

/// The objects inside a parsed block that might be an ask: the block
/// itself, its `ask`/`decision` field, or the entries of either as an
/// array (a model may batch several questions in one block).
fn ask_candidates(value: &Value) -> Vec<Value> {
    let inner = value
        .get("ask")
        .or_else(|| value.get("decision"))
        .unwrap_or(value);
    match inner {
        Value::Array(entries) => entries.clone(),
        other => vec![other.clone()],
    }
}

/// One `{question, options, multiSelect}` object → a decision, when it
/// carries both a prompt and at least one option.
fn decision_from_question(tool: &str, question: &Value) -> Option<AdapterEvent> {
    let prompt = str_field(question, "question")
        .or_else(|| str_field(question, "prompt"))
        .or_else(|| str_field(question, "header"))?;
    let options: Vec<String> = question
        .get("options")
        .and_then(Value::as_array)
        .map(|options| options.iter().filter_map(option_label).collect())
        .unwrap_or_default();
    if options.is_empty() {
        return None;
    }
    Some(AdapterEvent::Decision {
        tool: tool.to_owned(),
        prompt,
        options,
        multi_select: question
            .get("multiSelect")
            .or_else(|| question.get("multi_select"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

/// An option is either a labeled object or a bare string.
fn option_label(option: &Value) -> Option<String> {
    match option {
        Value::String(label) if !label.is_empty() => Some(label.clone()),
        other => str_field(other, "label").or_else(|| str_field(other, "name")),
    }
}

fn str_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

/// Every fenced code block's content (``` or ```json fences). An unclosed
/// fence yields nothing — a half-streamed block is not a decision yet.
fn fenced_blocks(text: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if !line.trim_start().starts_with("```") {
            continue;
        }
        let mut block = String::new();
        let mut closed = false;
        for inner in lines.by_ref() {
            if inner.trim_start().starts_with("```") {
                closed = true;
                break;
            }
            block.push_str(inner);
            block.push('\n');
        }
        if closed {
            blocks.push(block);
        }
    }
    blocks
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parts(event: &AdapterEvent) -> (&str, &str, &[String], bool) {
        match event {
            AdapterEvent::Decision {
                tool,
                prompt,
                options,
                multi_select,
            } => (tool, prompt, options, *multi_select),
            other => panic!("expected Decision, got {other:?}"),
        }
    }

    #[test]
    fn claude_ask_user_question_yields_one_decision_per_question() {
        let input = json!({"questions": [
            {"question": "Which database?", "multiSelect": false,
             "options": [{"label": "Postgres"}, {"label": "SQLite"}]},
            {"question": "Which extras?", "multiSelect": true,
             "options": [{"label": "Auth"}]},
            {"question": "No options", "options": []}
        ]});
        let events = from_tool_input("AskUserQuestion", &input);
        assert_eq!(
            events.len(),
            2,
            "the option-less question is not a decision"
        );
        assert_eq!(
            parts(&events[0]),
            (
                "AskUserQuestion",
                "Which database?",
                &["Postgres".to_owned(), "SQLite".to_owned()][..],
                false
            )
        );
        assert!(parts(&events[1]).3, "multiSelect carries through");
    }

    #[test]
    fn a_plan_becomes_approve_or_keep_planning() {
        let events = from_tool_input("ExitPlanMode", &json!({"plan": "1. scaffold"}));
        let (tool, prompt, options, _) = parts(&events[0]);
        assert_eq!(tool, "ExitPlanMode");
        assert!(prompt.contains("scaffold"));
        assert_eq!(options.len(), 2);
    }

    /// Shape-first matching: a provider that names its asking tool
    /// something else (codex approval prompts, F-04) still produces a
    /// decision as long as it passes a question and options.
    #[test]
    fn an_unknown_tool_with_a_question_and_options_still_asks() {
        let input = json!({
            "prompt": "Run `rm -rf build`?",
            "options": ["Approve", "Approve for the session", "Deny"]
        });
        let events = from_tool_input("exec_command_approval", &input);
        let (tool, prompt, options, _) = parts(&events[0]);
        assert_eq!(tool, "exec_command_approval");
        assert!(prompt.starts_with("Run"));
        assert_eq!(options.len(), 3, "bare string options are labels");
    }

    #[test]
    fn ordinary_tool_calls_are_not_decisions() {
        assert!(from_tool_input("Bash", &json!({"command": "ls"})).is_empty());
        assert!(from_tool_input("Read", &json!({"file_path": "a.rs"})).is_empty());
    }

    #[test]
    fn text_protocol_reads_fenced_ask_blocks() {
        let text = "Before I write the schema I need one call.\n\
                    \n\
                    ```json\n\
                    {\"ask\": {\"question\": \"Which database?\",\n\
                    \"options\": [\"Postgres\", \"SQLite\"], \"multiSelect\": false}}\n\
                    ```\n\
                    \n\
                    Tell me and I will continue.";
        let events = from_text(text);
        assert_eq!(events.len(), 1);
        let (tool, prompt, options, multi) = parts(&events[0]);
        assert_eq!(tool, "ask");
        assert_eq!(prompt, "Which database?");
        assert_eq!(options, &["Postgres".to_owned(), "SQLite".to_owned()][..]);
        assert!(!multi);
    }

    #[test]
    fn text_protocol_accepts_a_batch_of_questions() {
        let text = "```json\n\
                    {\"ask\": [\
                    {\"question\": \"Auth provider?\", \"options\": [\"Clerk\", \"Auth.js\"]},\
                    {\"question\": \"Extras?\", \"options\": [\"Billing\"], \"multi_select\": true}\
                    ]}\n\
                    ```";
        let events = from_text(text);
        assert_eq!(events.len(), 2);
        assert!(parts(&events[1]).3, "snake_case multi_select is accepted");
    }

    // -- frozen fixtures: real `codex exec --json` output (F-04 evidence) ---

    /// Corpus captured on 2026-08-22 with codex-cli 0.149.0 /
    /// gpt-5.6-terra. Absent on other machines: the test skips rather
    /// than fails, matching the claude fixture tests.
    fn codex_fixture(name: &str) -> Option<String> {
        let root =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../cli-codex-output");
        let mut runs: Vec<_> = std::fs::read_dir(root)
            .ok()?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect();
        runs.sort();
        let dir = runs.last()?.join("fixtures");
        std::fs::read_to_string(dir.join(name)).ok()
    }

    /// The text of every `agent_message` item in a codex JSONL transcript,
    /// which is where a text-protocol ask lands.
    fn codex_agent_messages(transcript: &str) -> String {
        transcript
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter_map(|event| {
                let item = event.get("item")?;
                (item.get("type").and_then(Value::as_str)? == "agent_message")
                    .then(|| str_field(item, "text"))?
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// codex has no asking tool at all — `codex exec` even rejects
    /// `--ask-for-approval` (fixture P4, exit 2) — so it asks the way agy
    /// does: a fenced block in the answer. This runs the parser over a real
    /// captured turn, not a synthetic string.
    #[test]
    fn real_codex_answer_carrying_an_ask_block_parses() {
        let Some(transcript) = codex_fixture("codex.P5-ask-protocol.stdout.jsonl") else {
            eprintln!("codex fixture corpus unavailable; skipping");
            return;
        };
        let events = from_text(&codex_agent_messages(&transcript));
        assert_eq!(events.len(), 1, "one question was asked");
        let (tool, prompt, options, multi) = parts(&events[0]);
        assert_eq!(tool, "ask");
        assert!(prompt.contains("database"), "prompt: {prompt}");
        assert_eq!(options, &["Postgres".to_owned(), "SQLite".to_owned()][..]);
        assert!(!multi);
    }

    /// Ordinary codex turns — plain answers, shell tool calls, and its
    /// `todo_list` plan surface — must never produce buttons. The plan
    /// items are progress reporting, not a human gate.
    #[test]
    fn ordinary_codex_turns_produce_no_decisions() {
        for name in [
            "codex.P1-basic.stdout.jsonl",
            "codex.P2-tools.stdout.jsonl",
            "codex.P3-plan.stdout.jsonl",
            "codex.P6-write.stdout.jsonl",
        ] {
            let Some(transcript) = codex_fixture(name) else {
                eprintln!("codex fixture corpus unavailable; skipping");
                return;
            };
            assert!(
                from_text(&codex_agent_messages(&transcript)).is_empty(),
                "{name} is not a decision"
            );
        }
    }

    #[test]
    fn plain_answers_and_broken_blocks_are_never_decisions() {
        assert!(from_text("Just an answer, no questions.").is_empty());
        assert!(from_text("```json\n{\"not\": \"an ask\"}\n```").is_empty());
        assert!(
            from_text("```json\n{\"ask\": {\"question\": \"x\", \"options\": []}}\n```").is_empty(),
            "no options, no buttons"
        );
        assert!(
            from_text("```json\n{\"ask\": {\"question\": \"half streamed\"").is_empty(),
            "an unclosed fence is not a decision yet"
        );
    }
}
