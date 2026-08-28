//! Daemon-owned project records.
//!
//! A project is deliberately a small durable pointer to an existing local
//! workspace.  It never runs an initializer or shell command: opening a
//! project is safe metadata/preflight work, and forgetting one only removes
//! AgentOS's record (never the user's files).

use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ProjectError {
    #[error("project path {0} is not an existing directory")]
    InvalidPath(String),
    #[error("project {0} was not found")]
    NotFound(String),
    #[error("project data is invalid: {0}")]
    Invalid(String),
    #[error("project parent path {0} is not an existing directory")]
    InvalidParent(String),
    #[error("project name {0:?} must be one safe directory name")]
    UnsafeName(String),
    #[error("starter {0:?} is not supported")]
    UnknownStarter(String),
    #[error("project destination {0} already exists")]
    DestinationExists(String),
    #[error("could not {operation} at {path}: {source}")]
    ScaffoldIo {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("git init failed in {path} (status {status}): {stderr}")]
    GitInit {
        path: String,
        status: String,
        stderr: String,
    },
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Persisted, daemon-owned project metadata. `sessionDefaults` is kept
/// separate from transient session overrides; it is only applied when a
/// caller explicitly selects this project.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProjectRecord {
    pub id: String,
    pub name: String,
    pub root_path: String,
    #[serde(default = "default_object")]
    pub session_defaults: Value,
    #[serde(default = "default_object")]
    pub settings: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_opened_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateProject {
    pub root_path: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default = "default_object")]
    pub session_defaults: Value,
    #[serde(default = "default_object")]
    pub settings: Value,
}

/// Inputs for the safe, built-in project scaffold operation.  The daemon
/// deliberately accepts no arbitrary template URL or command: starter IDs
/// select only the small file sets in this module.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScaffoldProject {
    pub parent_path: String,
    pub name: String,
    pub starter_id: String,
    #[serde(default = "default_object")]
    pub session_defaults: Value,
    #[serde(default = "default_object")]
    pub settings: Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProject {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub session_defaults: Option<Value>,
    #[serde(default)]
    pub settings: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ProjectStore {
    path: PathBuf,
}

impl ProjectStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, ProjectError> {
        let store = Self { path: path.into() };
        let conn = store.connection()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                root_path TEXT NOT NULL UNIQUE,
                session_defaults_json TEXT NOT NULL,
                settings_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_opened_at TEXT
            );
            CREATE INDEX IF NOT EXISTS projects_last_opened_idx
                ON projects(last_opened_at DESC);",
        )?;
        Ok(store)
    }

    pub fn list(&self) -> Result<Vec<ProjectRecord>, ProjectError> {
        let conn = self.connection()?;
        let mut statement = conn.prepare(
            "SELECT id, name, root_path, session_defaults_json, settings_json,
                    created_at, updated_at, last_opened_at
             FROM projects ORDER BY last_opened_at DESC NULLS LAST, updated_at DESC",
        )?;
        let rows = statement
            .query_map([], project_from_row)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(ProjectError::from)?;
        Ok(rows)
    }

    pub fn create(&self, input: CreateProject) -> Result<ProjectRecord, ProjectError> {
        let root = normalized_workspace(&input.root_path)?;
        ensure_object(&input.session_defaults, "sessionDefaults")?;
        ensure_object(&input.settings, "settings")?;
        let name = input
            .name
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| {
                root.file_name()
                    .and_then(|part| part.to_str())
                    .unwrap_or("Project")
                    .to_owned()
            });
        if name.chars().count() > 120 {
            return Err(ProjectError::Invalid(
                "name must be at most 120 characters".to_owned(),
            ));
        }
        let now = Utc::now();
        let record = ProjectRecord {
            id: Uuid::now_v7().to_string(),
            name,
            root_path: root.display().to_string(),
            session_defaults: input.session_defaults,
            settings: input.settings,
            created_at: now,
            updated_at: now,
            last_opened_at: None,
        };
        let conn = self.connection()?;
        conn.execute(
            "INSERT INTO projects (id, name, root_path, session_defaults_json, settings_json,
              created_at, updated_at, last_opened_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)",
            params![
                record.id,
                record.name,
                record.root_path,
                serde_json::to_string(&record.session_defaults)?,
                serde_json::to_string(&record.settings)?,
                record.created_at.to_rfc3339(),
                record.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(record)
    }

    /// Create one of the daemon's built-in starter layouts, initialize a git
    /// repository, atomically publish it, and only then record it.  All file
    /// writes happen in a unique sibling staging directory so a partially
    /// written starter is never visible at the requested destination.
    pub fn scaffold(&self, input: ScaffoldProject) -> Result<ProjectRecord, ProjectError> {
        ensure_object(&input.session_defaults, "sessionDefaults")?;
        ensure_object(&input.settings, "settings")?;
        validate_directory_name(&input.name)?;
        let starter = Starter::from_id(&input.starter_id)?;
        let parent = normalized_parent(&input.parent_path)?;
        let destination = parent.join(&input.name);
        ensure_absent_destination(&destination)?;

        let staging = parent.join(format!(
            ".agentos-stage-{}-{}",
            input.name,
            Uuid::now_v7().simple()
        ));
        std::fs::create_dir(&staging).map_err(|source| ProjectError::ScaffoldIo {
            operation: "create staging directory",
            path: staging.display().to_string(),
            source,
        })?;

        let result = (|| {
            starter.write_files(&staging, &input.name)?;
            initialize_git(&staging)?;
            std::fs::rename(&staging, &destination).map_err(|source| ProjectError::ScaffoldIo {
                operation: "publish scaffolded project",
                path: destination.display().to_string(),
                source,
            })?;
            self.create(CreateProject {
                root_path: destination.display().to_string(),
                name: Some(input.name),
                session_defaults: input.session_defaults,
                settings: input.settings,
            })
        })();

        if let Err(error) = &result {
            cleanup_staging(&parent, &staging);
            tracing::warn!(
                parent = %parent.display(),
                staging = %staging.display(),
                error = %error,
                "project scaffold failed"
            );
        }
        result
    }

    pub fn get(&self, id: &str) -> Result<ProjectRecord, ProjectError> {
        self.lookup(id)?
            .ok_or_else(|| ProjectError::NotFound(id.to_owned()))
    }

    pub fn open_project(&self, id: &str) -> Result<ProjectRecord, ProjectError> {
        let record = self.get(id)?;
        normalized_workspace(&record.root_path)?;
        let now = Utc::now();
        let conn = self.connection()?;
        conn.execute(
            "UPDATE projects SET last_opened_at = ?1, updated_at = ?1 WHERE id = ?2",
            params![now.to_rfc3339(), id],
        )?;
        self.get(id)
    }

    pub fn update(&self, input: UpdateProject) -> Result<ProjectRecord, ProjectError> {
        let current = self.get(&input.id)?;
        let name = input.name.unwrap_or(current.name);
        if name.trim().is_empty() || name.chars().count() > 120 {
            return Err(ProjectError::Invalid(
                "name must be 1..=120 characters".to_owned(),
            ));
        }
        let defaults = input.session_defaults.unwrap_or(current.session_defaults);
        let settings = input.settings.unwrap_or(current.settings);
        ensure_object(&defaults, "sessionDefaults")?;
        ensure_object(&settings, "settings")?;
        let now = Utc::now();
        let conn = self.connection()?;
        conn.execute(
            "UPDATE projects SET name = ?1, session_defaults_json = ?2, settings_json = ?3,
                    updated_at = ?4 WHERE id = ?5",
            params![
                name,
                serde_json::to_string(&defaults)?,
                serde_json::to_string(&settings)?,
                now.to_rfc3339(),
                input.id,
            ],
        )?;
        self.get(&input.id)
    }

    /// Forgetting a project is intentionally metadata-only.
    pub fn forget(&self, id: &str) -> Result<(), ProjectError> {
        let conn = self.connection()?;
        if conn.execute("DELETE FROM projects WHERE id = ?1", [id])? == 0 {
            return Err(ProjectError::NotFound(id.to_owned()));
        }
        Ok(())
    }

    /// Static starter metadata. Every starter is written by this module and
    /// initialized only with structured `git init` arguments.
    pub fn starters() -> Value {
        json!({"starters": [
            {"id":"blank-git", "label":"Blank Git project", "language":"none", "safe": true},
            {"id":"nextjs", "label":"Next.js app", "language":"javascript", "safe": true},
            {"id":"react-vite", "label":"React + Vite app", "language":"javascript", "safe": true},
            {"id":"node-api", "label":"Node API", "language":"javascript", "safe": true},
            {"id":"python", "label":"Python app", "language":"python", "safe": true},
            {"id":"rust", "label":"Rust binary", "language":"rust", "safe": true}
        ]})
    }

    /// Filesystem-only preflight used by the project picker. It does not run
    /// git, package managers, hooks, or a user-provided command.
    pub fn preflight(path: &str) -> Result<Value, ProjectError> {
        let root = normalized_workspace(path)?;
        Ok(json!({
            "rootPath": root.display().to_string(),
            "exists": true,
            "isGitRepository": root.join(".git").exists(),
            "safe": true,
            "checks": ["directory_exists", "metadata_only"]
        }))
    }

    fn lookup(&self, id: &str) -> Result<Option<ProjectRecord>, ProjectError> {
        let conn = self.connection()?;
        conn.query_row(
            "SELECT id, name, root_path, session_defaults_json, settings_json,
                    created_at, updated_at, last_opened_at FROM projects WHERE id = ?1",
            [id],
            project_from_row,
        )
        .optional()
        .map_err(ProjectError::from)
    }

    fn connection(&self) -> Result<Connection, ProjectError> {
        let conn = Connection::open(&self.path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(conn)
    }
}

fn project_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProjectRecord> {
    let parse = |index| -> rusqlite::Result<Value> {
        let raw: String = row.get(index)?;
        serde_json::from_str(&raw).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })
    };
    let timestamp = |index| -> rusqlite::Result<DateTime<Utc>> {
        let raw: String = row.get(index)?;
        DateTime::parse_from_rfc3339(&raw)
            .map(|value| value.with_timezone(&Utc))
            .map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })
    };
    let optional_timestamp = |index| -> rusqlite::Result<Option<DateTime<Utc>>> {
        let raw: Option<String> = row.get(index)?;
        raw.map(|raw| DateTime::parse_from_rfc3339(&raw).map(|value| value.with_timezone(&Utc)))
            .transpose()
            .map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })
    };
    Ok(ProjectRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        root_path: row.get(2)?,
        session_defaults: parse(3)?,
        settings: parse(4)?,
        created_at: timestamp(5)?,
        updated_at: timestamp(6)?,
        last_opened_at: optional_timestamp(7)?,
    })
}

#[derive(Debug, Clone, Copy)]
enum Starter {
    BlankGit,
    Nextjs,
    ReactVite,
    NodeApi,
    Python,
    Rust,
}

impl Starter {
    fn from_id(id: &str) -> Result<Self, ProjectError> {
        match id {
            "blank-git" => Ok(Self::BlankGit),
            "nextjs" => Ok(Self::Nextjs),
            "react-vite" => Ok(Self::ReactVite),
            "node-api" => Ok(Self::NodeApi),
            "python" => Ok(Self::Python),
            "rust" => Ok(Self::Rust),
            _ => Err(ProjectError::UnknownStarter(id.to_owned())),
        }
    }

    fn write_files(self, root: &Path, name: &str) -> Result<(), ProjectError> {
        match self {
            Self::BlankGit => {
                write_starter_file(
                    root,
                    "README.md",
                    &format!("# {name}\n\nA new AgentOS project.\n"),
                )?;
            }
            Self::Nextjs => {
                write_starter_file(
                    root,
                    "package.json",
                    &format!(
                        r#"{{
  "name": "{name}",
  "private": true,
  "scripts": {{ "dev": "next dev", "build": "next build", "start": "next start" }},
  "dependencies": {{ "next": "15.0.0", "react": "19.0.0", "react-dom": "19.0.0" }}
}}
"#
                    ),
                )?;
                write_starter_file(
                    root,
                    "app/layout.js",
                    "export default function RootLayout({ children }) {\n  return <html lang=\"en\"><body>{children}</body></html>;\n}\n",
                )?;
                write_starter_file(
                    root,
                    "app/page.js",
                    "export default function Home() {\n  return <main><h1>New AgentOS project</h1></main>;\n}\n",
                )?;
            }
            Self::ReactVite => {
                write_starter_file(
                    root,
                    "package.json",
                    &format!(
                        r#"{{
  "name": "{name}",
  "private": true,
  "scripts": {{ "dev": "vite", "build": "vite build", "preview": "vite preview" }},
  "dependencies": {{ "@vitejs/plugin-react": "latest", "vite": "latest", "react": "latest", "react-dom": "latest" }},
  "devDependencies": {{}}
}}
"#
                    ),
                )?;
                write_starter_file(root, "index.html", "<div id=\"root\"></div><script type=\"module\" src=\"/src/main.jsx\"></script>\n")?;
                write_starter_file(
                    root,
                    "src/main.jsx",
                    "import { createRoot } from 'react-dom/client';\n\ncreateRoot(document.getElementById('root')).render(<h1>New AgentOS project</h1>);\n",
                )?;
            }
            Self::NodeApi => {
                write_starter_file(
                    root,
                    "package.json",
                    &format!(
                        r#"{{
  "name": "{name}",
  "private": true,
  "scripts": {{ "start": "node src/server.js" }}
}}
"#
                    ),
                )?;
                write_starter_file(
                    root,
                    "src/server.js",
                    "const http = require('node:http');\n\nconst server = http.createServer((_request, response) => {\n  response.writeHead(200, { 'content-type': 'application/json' });\n  response.end(JSON.stringify({ ok: true }));\n});\n\nserver.listen(process.env.PORT || 3000);\n",
                )?;
            }
            Self::Python => {
                write_starter_file(
                    root,
                    "main.py",
                    "def main():\n    print(\"Hello from AgentOS\")\n\n\nif __name__ == \"__main__\":\n    main()\n",
                )?;
                write_starter_file(root, "requirements.txt", "")?;
            }
            Self::Rust => {
                write_starter_file(
                    root,
                    "Cargo.toml",
                    &format!(
                        "[package]\nname = \"{}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\n",
                        cargo_package_name(name)
                    ),
                )?;
                write_starter_file(
                    root,
                    "src/main.rs",
                    "fn main() {\n    println!(\"Hello from AgentOS!\");\n}\n",
                )?;
            }
        }
        Ok(())
    }
}

fn normalized_parent(path: &str) -> Result<PathBuf, ProjectError> {
    let parent = Path::new(path);
    if !parent.is_dir() {
        return Err(ProjectError::InvalidParent(parent.display().to_string()));
    }
    std::fs::canonicalize(parent)
        .map_err(|_| ProjectError::InvalidParent(parent.display().to_string()))
}

fn validate_directory_name(name: &str) -> Result<(), ProjectError> {
    let path = Path::new(name);
    let components = path.components().collect::<Vec<_>>();
    let is_one_component =
        matches!(components.as_slice(), [Component::Normal(part)] if *part == OsStr::new(name));
    let invalid_windows_char = name.chars().any(|character| {
        character.is_control()
            || matches!(
                character,
                '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*'
            )
    });
    let has_bad_ending = name.ends_with('.') || name.ends_with(' ');
    if !is_one_component || invalid_windows_char || has_bad_ending || name.chars().count() > 120 {
        return Err(ProjectError::UnsafeName(name.to_owned()));
    }
    Ok(())
}

fn ensure_absent_destination(destination: &Path) -> Result<(), ProjectError> {
    match std::fs::symlink_metadata(destination) {
        Ok(_) => Err(ProjectError::DestinationExists(
            destination.display().to_string(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ProjectError::ScaffoldIo {
            operation: "inspect project destination",
            path: destination.display().to_string(),
            source,
        }),
    }
}

fn write_starter_file(root: &Path, relative: &str, contents: &str) -> Result<(), ProjectError> {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ProjectError::ScaffoldIo {
            operation: "create starter directory",
            path: parent.display().to_string(),
            source,
        })?;
    }
    std::fs::write(&path, contents).map_err(|source| ProjectError::ScaffoldIo {
        operation: "write starter file",
        path: path.display().to_string(),
        source,
    })
}

fn initialize_git(root: &Path) -> Result<(), ProjectError> {
    let output = Command::new("git")
        .arg("init")
        .arg("--quiet")
        .current_dir(root)
        .output()
        .map_err(|source| ProjectError::ScaffoldIo {
            operation: "run git init",
            path: root.display().to_string(),
            source,
        })?;
    if output.status.success() {
        return Ok(());
    }
    Err(ProjectError::GitInit {
        path: root.display().to_string(),
        status: output.status.to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
    })
}

/// Only remove a staging directory that is still a direct, non-symlink
/// child of the canonical parent and still uses our generated prefix.
fn cleanup_staging(parent: &Path, staging: &Path) {
    let is_expected_name = staging
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(".agentos-stage-"));
    let is_direct_child = staging.parent() == Some(parent);
    let metadata = std::fs::symlink_metadata(staging);
    if !is_expected_name || !is_direct_child {
        tracing::warn!(staging = %staging.display(), "refusing to clean an unverified scaffold staging path");
        return;
    }
    match metadata {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            if let Err(error) = std::fs::remove_dir_all(staging) {
                tracing::warn!(staging = %staging.display(), %error, "could not remove scaffold staging directory");
            }
        }
        Ok(_) => {
            tracing::warn!(staging = %staging.display(), "refusing to clean a non-directory or symlink scaffold staging path")
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(staging = %staging.display(), %error, "could not inspect scaffold staging directory for cleanup")
        }
    }
}

fn cargo_package_name(name: &str) -> String {
    let normalized = name
        .chars()
        .map(|character| match character {
            'A'..='Z' => character.to_ascii_lowercase(),
            'a'..='z' | '0'..='9' | '-' => character,
            _ => '-',
        })
        .collect::<String>();
    let normalized = normalized.trim_matches('-');
    if normalized.is_empty() {
        "agentos-project".to_owned()
    } else {
        normalized.to_owned()
    }
}

pub fn normalized_workspace(path: &str) -> Result<PathBuf, ProjectError> {
    let path = Path::new(path);
    if !path.is_dir() {
        return Err(ProjectError::InvalidPath(path.display().to_string()));
    }
    std::fs::canonicalize(path).map_err(|_| ProjectError::InvalidPath(path.display().to_string()))
}

fn ensure_object(value: &Value, field: &str) -> Result<(), ProjectError> {
    if value.is_object() {
        Ok(())
    } else {
        Err(ProjectError::Invalid(format!("{field} must be an object")))
    }
}

fn default_object() -> Value {
    json!({})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_records_round_trip_and_forget_never_touches_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let store = ProjectStore::open(dir.path().join("projects.db")).unwrap();
        let created = store
            .create(CreateProject {
                root_path: workspace.display().to_string(),
                name: None,
                session_defaults: json!({"model":"mock-model-1"}),
                settings: json!({}),
            })
            .unwrap();
        let opened = store.open_project(&created.id).unwrap();
        assert!(opened.last_opened_at.is_some());
        store.forget(&created.id).unwrap();
        assert!(workspace.is_dir(), "forget must never delete a workspace");
    }

    #[test]
    fn scaffold_publishes_a_git_project_then_registers_it() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("projects");
        std::fs::create_dir(&parent).unwrap();
        let store = ProjectStore::open(dir.path().join("projects.db")).unwrap();

        let record = store
            .scaffold(ScaffoldProject {
                parent_path: parent.display().to_string(),
                name: "hello-agentos".to_owned(),
                starter_id: "rust".to_owned(),
                session_defaults: json!({"model":"mock"}),
                settings: json!({"theme":"dark"}),
            })
            .expect("scaffold succeeds");

        let project = parent.join("hello-agentos");
        assert!(project.join(".git").is_dir(), "git was initialized");
        assert!(project.join("Cargo.toml").is_file(), "starter was written");
        assert_eq!(
            record.root_path,
            std::fs::canonicalize(&project)
                .unwrap()
                .display()
                .to_string()
        );
        assert_eq!(store.list().unwrap(), vec![record]);
    }

    #[test]
    fn scaffold_rejects_traversal_and_existing_destinations() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("projects");
        std::fs::create_dir(&parent).unwrap();
        let store = ProjectStore::open(dir.path().join("projects.db")).unwrap();
        let base = |name: &str| ScaffoldProject {
            parent_path: parent.display().to_string(),
            name: name.to_owned(),
            starter_id: "blank-git".to_owned(),
            session_defaults: json!({}),
            settings: json!({}),
        };

        assert!(matches!(
            store.scaffold(base("../escape")),
            Err(ProjectError::UnsafeName(_))
        ));
        std::fs::create_dir(parent.join("already-there")).unwrap();
        assert!(matches!(
            store.scaffold(base("already-there")),
            Err(ProjectError::DestinationExists(_))
        ));
        assert!(
            store.list().unwrap().is_empty(),
            "failed scaffolds are not registered"
        );
    }
}
