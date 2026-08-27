//! Importing skills that already exist on disk.
//!
//! Claude Code keeps its skills as `~/.claude/skills/<id>/SKILL.md` — a YAML
//! frontmatter block (`name`, `description`, …) followed by a markdown body.
//! That shape maps onto a [`SkillRecord`] almost exactly, but nothing in this
//! daemon ever read the directory, so a machine with a thousand well-written
//! skills still offered agents only the seeded handful.
//!
//! Discovery is deliberately cheap: the frontmatter lives in the first few
//! hundred bytes, so listing reads only a head buffer and takes the size from
//! the directory entry. Bodies are read once, at import.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use agentos_agents::{SkillRecord, BODY_MAX_CHARS};
use chrono::Utc;
use serde::Serialize;

/// Environment variable overriding where skills are discovered.
pub const SKILL_SOURCE_ENV: &str = "AGENTOS_SKILL_SOURCE";

/// Bytes read per file while listing — enough for any frontmatter block.
const HEAD_BYTES: usize = 4096;

/// Frontmatter values are capped by the registry; mirror the limits here so
/// a long description costs a truncation, not a refused import.
const NAME_MAX: usize = 80;
const DESCRIPTION_MAX: usize = 500;

/// One skill found on disk, without its body.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    /// Body size in bytes, from the directory entry.
    pub size_bytes: u64,
    /// Body exceeds the registry's per-skill budget and cannot be imported.
    pub oversize: bool,
    /// A skill with this id is already in the registry.
    pub installed: bool,
}

/// Where skills are discovered: `AGENTOS_SKILL_SOURCE`, else
/// `~/.claude/skills`.
pub fn source_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os(SKILL_SOURCE_ENV) {
        return PathBuf::from(dir);
    }
    home_dir().join(".claude").join("skills")
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// List every importable skill under `dir`, sorted by id. A directory that
/// does not exist is not an error — it just holds nothing.
pub fn discover(dir: &Path, installed: &[String]) -> Vec<DiscoveredSkill> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };

    let mut found: Vec<DiscoveredSkill> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let id = entry.file_name().to_string_lossy().to_string();
            if !valid_slug(&id) {
                return None;
            }
            let path = entry.path().join("SKILL.md");
            let size_bytes = fs::metadata(&path).ok()?.len();
            let head = read_head(&path)?;
            let (name, description, _) = split_frontmatter(&head);
            // The head buffer may cut the body short; only the frontmatter
            // is trusted here, and the size comes from the entry.
            let body_bytes = size_bytes.saturating_sub(frontmatter_len(&head) as u64);
            Some(DiscoveredSkill {
                name: if name.is_empty() { prettify(&id) } else { name },
                description,
                oversize: body_bytes > BODY_MAX_CHARS as u64,
                installed: installed.iter().any(|existing| existing == &id),
                size_bytes: body_bytes,
                id,
            })
        })
        .collect();

    found.sort_by(|a, b| a.id.cmp(&b.id));
    found
}

/// Write the OS's own skills into the library as `SKILL.md` files.
///
/// There is one skill store, and it is the folder. The bodies in
/// `agentos-agents::builtin_skills` are the *authoring* source; this puts
/// them where every other skill lives so they are edited in the same place
/// and offered by the same catalog.
///
/// An existing file is never overwritten — the library is the user's, and
/// a hand-edited method must survive a daemon restart.
pub fn export_builtins(dir: &Path, skills: &[SkillRecord]) -> Result<Vec<String>, String> {
    fs::create_dir_all(dir).map_err(|err| format!("{}: {err}", dir.display()))?;

    let mut written = Vec::new();
    for skill in skills {
        let path = dir.join(&skill.id).join("SKILL.md");
        if path.exists() {
            continue;
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| format!("{}: {err}", parent.display()))?;
        }
        fs::write(&path, render(skill)).map_err(|err| format!("{}: {err}", path.display()))?;
        written.push(skill.id.clone());
    }
    Ok(written)
}

/// Write one skill to the library, replacing what is there.
///
/// Used when a skill is created or edited through the API: without it the
/// registry and the library drift apart, and the next startup sync would
/// quietly undo the edit.
pub fn write_one(dir: &Path, skill: &SkillRecord) -> Result<(), String> {
    let path = dir.join(&skill.id).join("SKILL.md");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| format!("{}: {err}", parent.display()))?;
    }
    fs::write(&path, render(skill)).map_err(|err| format!("{}: {err}", path.display()))
}

/// Remove one skill's mirrored directory from the library.
///
/// The counterpart to [`write_one`]: a registry delete that skipped this
/// left an orphan `SKILL.md` that the user's own tooling still lists as an
/// installed skill. Absent is success — the mirror may never have existed.
pub fn remove_one(dir: &Path, id: &str) -> Result<(), String> {
    let path = dir.join(id);
    match fs::remove_dir_all(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("{}: {err}", path.display())),
    }
}

/// A skill as a SKILL.md: frontmatter, then the body.
fn render(skill: &SkillRecord) -> String {
    format!(
        "---\nname: {}\ndescription: {}\nsource: agent-engineering-os\n---\n\n{}\n",
        yaml_scalar(&skill.name),
        yaml_scalar(&skill.description),
        skill.body.trim_end(),
    )
}

/// Quote a frontmatter value when it could otherwise be misread.
fn yaml_scalar(value: &str) -> String {
    let needs_quotes = value.is_empty()
        || value.contains(':')
        || value.contains('#')
        || value.starts_with(['"', '\'', '[', '{', '-', '&', '*', '!', '|', '>', '%', '@']);
    if needs_quotes {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        value.to_owned()
    }
}

/// Bring the library and the registry into agreement at startup.
///
/// Exports any OS skill the library is missing, then imports every id the
/// library holds for those skills back into the registry so the seeded
/// agents can reference them. Returns `(exported, imported)`.
pub fn sync_library(
    registry: &agentos_agents::AgentRegistry,
    dir: &Path,
    builtins: &[SkillRecord],
) -> Result<(Vec<String>, Vec<String>), String> {
    let exported = export_builtins(dir, builtins)?;

    let mut imported = Vec::new();
    for skill in builtins {
        // The file wins. Read it back rather than inserting the shipped
        // struct, and refresh an existing row rather than leaving a stale
        // copy behind — one store means edits to the library are what the
        // agents actually run.
        let record = load(dir, &skill.id)?;
        let existing = registry.get_skill(&skill.id).map_err(|e| e.to_string())?;
        match existing {
            Some(current) if current.body == record.body && current.name == record.name => continue,
            Some(_) => {
                registry.update_skill(record).map_err(|e| e.to_string())?;
            }
            None => {
                registry.create_skill(record).map_err(|e| e.to_string())?;
            }
        }
        imported.push(skill.id.clone());
    }
    Ok((exported, imported))
}

/// Read one skill off disk into a registry record.
pub fn load(dir: &Path, id: &str) -> Result<SkillRecord, String> {
    if !valid_slug(id) {
        return Err(format!("{id:?} is not a valid skill id"));
    }
    let path = dir.join(id).join("SKILL.md");
    let raw = fs::read_to_string(&path).map_err(|err| format!("{}: {err}", path.display()))?;
    let (name, description, body) = split_frontmatter(&raw);

    let body = body.trim().to_owned();
    if body.is_empty() {
        return Err(format!("{id}: SKILL.md has no body below its frontmatter"));
    }
    if body.chars().count() > BODY_MAX_CHARS {
        // Truncating a method document mid-sentence would install a broken
        // directive, which is worse than not installing it.
        return Err(format!(
            "{id}: body is {} chars, over the {BODY_MAX_CHARS} limit",
            body.chars().count()
        ));
    }

    let now = Utc::now();
    Ok(SkillRecord {
        name: cap(if name.is_empty() { prettify(id) } else { name }, NAME_MAX),
        description: cap(description, DESCRIPTION_MAX),
        body,
        builtin: false,
        created_at: now,
        updated_at: now,
        id: id.to_owned(),
    })
}

fn read_head(path: &Path) -> Option<String> {
    let mut file = fs::File::open(path).ok()?;
    let mut buf = vec![0u8; HEAD_BYTES];
    let read = file.read(&mut buf).ok()?;
    buf.truncate(read);
    Some(String::from_utf8_lossy(&buf).into_owned())
}

/// Byte length of the leading frontmatter block, delimiters included.
fn frontmatter_len(text: &str) -> usize {
    let Some(rest) = text.strip_prefix("---") else {
        return 0;
    };
    match rest.find("\n---") {
        Some(end) => 3 + end + 4,
        None => 0,
    }
}

/// Split `name`, `description` and the body out of a SKILL.md.
///
/// A hand-rolled reader rather than a YAML dependency: the block is a flat
/// map of scalars, and the only two keys that matter are strings.
fn split_frontmatter(text: &str) -> (String, String, String) {
    let Some(rest) = text.strip_prefix("---") else {
        return (String::new(), String::new(), text.to_owned());
    };
    let Some(end) = rest.find("\n---") else {
        return (String::new(), String::new(), text.to_owned());
    };

    let block = &rest[..end];
    let body = rest[end + 4..].trim_start_matches('\n').to_owned();

    let mut name = String::new();
    let mut description = String::new();
    for line in block.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        // Only top-level keys: an indented line belongs to a nested value.
        if key.starts_with(char::is_whitespace) {
            continue;
        }
        let value = unquote(value.trim());
        match key.trim() {
            "name" => name = value,
            "description" => description = value,
            _ => {}
        }
    }
    (name, description, body)
}

/// Inverse of [`yaml_scalar`], and tolerant of the plain quoting the rest
/// of the library uses. Double-quoted values carry backslash escapes;
/// single-quoted ones are taken literally.
fn unquote(value: &str) -> String {
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        let inner = &value[1..value.len() - 1];
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                out.push(chars.next().unwrap_or('\\'));
            } else {
                out.push(c);
            }
        }
        return out;
    }
    if value.len() >= 2 && value.starts_with('\'') && value.ends_with('\'') {
        return value[1..value.len() - 1].to_owned();
    }
    value.to_owned()
}

fn cap(mut value: String, max: usize) -> String {
    if value.chars().count() > max {
        value = value.chars().take(max - 1).collect::<String>();
        value.push('…');
    }
    value
}

/// `some-skill.v2` → `Some Skill.v2`, for a directory with no `name:`.
fn prettify(id: &str) -> String {
    id.split('-')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Mirrors the registry's slug rule so discovery never lists an id the
/// registry would refuse.
fn valid_slug(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_' || c == '.'
        })
        && id.chars().next().is_some_and(|c| c.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_skill(root: &Path, id: &str, contents: &str) {
        let dir = root.join(id);
        fs::create_dir_all(&dir).expect("mkdir");
        fs::write(dir.join("SKILL.md"), contents).expect("write");
    }

    #[test]
    fn frontmatter_yields_name_description_and_body() {
        let text = "---\nname: react-patterns\ndescription: \"Hooks, composition.\"\n\
                    risk: unknown\n---\n\n# React Patterns\n\nBody here.\n";
        let (name, description, body) = split_frontmatter(text);
        assert_eq!(name, "react-patterns");
        assert_eq!(description, "Hooks, composition.");
        assert!(body.starts_with("# React Patterns"));
        assert!(
            !body.contains("risk:"),
            "frontmatter must not leak into the body"
        );
    }

    #[test]
    fn single_quoted_and_colon_bearing_values_survive() {
        let text =
            "---\nname: 'python-pro'\ndescription: Master Python: async, uv, ruff.\n---\nBody.";
        let (name, description, _) = split_frontmatter(text);
        assert_eq!(name, "python-pro");
        assert_eq!(description, "Master Python: async, uv, ruff.");
    }

    #[test]
    fn a_file_without_frontmatter_is_all_body() {
        let (name, description, body) = split_frontmatter("# Just markdown\n");
        assert!(name.is_empty() && description.is_empty());
        assert_eq!(body, "# Just markdown\n");
    }

    #[test]
    fn discovery_lists_ids_and_flags_what_is_already_installed() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_skill(
            dir.path(),
            "react-patterns",
            "---\nname: React Patterns\ndescription: d\n---\nBody",
        );
        write_skill(
            dir.path(),
            "python-pro",
            "---\nname: Python Pro\ndescription: d\n---\nBody",
        );
        // Uppercase is not a valid registry slug, so it is never offered.
        write_skill(dir.path(), "NotASlug", "---\nname: No\n---\nBody");

        let found = discover(dir.path(), &["python-pro".to_owned()]);
        let ids: Vec<&str> = found.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, vec!["python-pro", "react-patterns"]);
        assert!(found[0].installed, "already in the registry");
        assert!(!found[1].installed);
    }

    #[test]
    fn a_missing_source_directory_is_empty_not_an_error() {
        assert!(discover(Path::new("no/such/place"), &[]).is_empty());
    }

    #[test]
    fn load_builds_a_registry_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_skill(
            dir.path(),
            "react-patterns",
            "---\nname: React Patterns\ndescription: Hooks and composition.\n---\n# Method\n\nDo the thing.",
        );
        let record = load(dir.path(), "react-patterns").expect("loads");
        assert_eq!(record.id, "react-patterns");
        assert_eq!(record.name, "React Patterns");
        assert!(record.body.starts_with("# Method"));
        assert!(!record.builtin, "an imported skill is never builtin");
        record.validate().expect("passes registry validation");
    }

    /// Truncating a method document mid-sentence installs a broken
    /// directive, so an oversized body is refused with its actual size.
    #[test]
    fn an_oversized_body_is_refused_rather_than_truncated() {
        let dir = tempfile::tempdir().expect("tempdir");
        let huge = "x".repeat(BODY_MAX_CHARS + 500);
        write_skill(dir.path(), "huge", &format!("---\nname: Huge\n---\n{huge}"));
        let err = load(dir.path(), "huge").expect_err("refused");
        assert!(err.contains("over the"), "{err}");

        let found = discover(dir.path(), &[]);
        assert!(found[0].oversize, "listing must flag it before you try");
    }

    /// The OS's own skills go into the library and come back out unchanged:
    /// one store means export and import must be exact inverses.
    #[test]
    fn exported_builtins_round_trip_through_the_library() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = Utc::now();
        let original = SkillRecord {
            id: "agent-creation".to_owned(),
            name: "Agent Creator".to_owned(),
            // A colon and quotes: the frontmatter must survive both.
            description: "Interviews you: drafts \"agents\" for the registry.".to_owned(),
            body: "# Agent Creator — Method\n\n1. Understand the need.\n".to_owned(),
            builtin: true,
            created_at: now,
            updated_at: now,
        };

        let written = export_builtins(dir.path(), std::slice::from_ref(&original)).expect("export");
        assert_eq!(written, vec!["agent-creation".to_owned()]);

        let back = load(dir.path(), "agent-creation").expect("import");
        assert_eq!(back.name, original.name);
        assert_eq!(back.description, original.description);
        assert_eq!(back.body, original.body.trim());
        back.validate().expect("valid");
    }

    #[test]
    fn ponytail_builtin_exports_with_provenance_and_validates() {
        let dir = tempfile::tempdir().expect("tempdir");
        let builtins = agentos_agents::builtin_skills();
        let written = export_builtins(dir.path(), &builtins).expect("export");
        assert!(written.iter().any(|id| id == "ponytail"));

        let skill = load(dir.path(), "ponytail").expect("load");
        skill.validate().expect("valid skill");
        assert!(skill.body.contains("Dietrich Gebert (MIT)"));
        assert!(skill
            .body
            .contains("https://github.com/DietrichGebert/ponytail"));
    }

    /// The library belongs to the user: a hand-edited method must survive a
    /// restart, so export never clobbers a file that already exists.
    #[test]
    fn export_never_overwrites_an_existing_skill() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_skill(
            dir.path(),
            "agent-creation",
            "---\nname: Mine\n---\nMy own method.",
        );

        let now = Utc::now();
        let shipped = SkillRecord {
            id: "agent-creation".to_owned(),
            name: "Shipped".to_owned(),
            description: "d".to_owned(),
            body: "Shipped body".to_owned(),
            builtin: true,
            created_at: now,
            updated_at: now,
        };
        let written = export_builtins(dir.path(), std::slice::from_ref(&shipped)).expect("export");
        assert!(written.is_empty(), "an existing file is left alone");
        assert_eq!(
            load(dir.path(), "agent-creation").expect("load").body,
            "My own method."
        );
    }

    #[test]
    fn a_directory_without_a_name_falls_back_to_its_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_skill(dir.path(), "seo-audit", "---\nrisk: unknown\n---\nBody");
        assert_eq!(
            load(dir.path(), "seo-audit").expect("loads").name,
            "Seo Audit"
        );
    }
}
