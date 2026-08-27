// Tauri 2 library entry point with Dialog plugin
use std::{fs, path::Path, process::Command};

fn project_directory(path: &str) -> Result<&Path, String> {
    let directory = Path::new(path);
    if !directory.is_absolute() || !directory.is_dir() || directory.parent().is_none() {
        return Err("Choose an existing, non-root project folder.".into());
    }
    Ok(directory)
}

fn folder_name(name: &str) -> Result<&str, String> {
    let name = name.trim();
    if name.is_empty()
        || matches!(name, "." | "..")
        || name.contains(['\\', '/', ':', '\0'])
    {
        return Err("Enter a valid folder name without path characters.".into());
    }
    Ok(name)
}

#[tauri::command]
fn setup_project_repository(path: String) -> Result<(), String> {
    let directory = project_directory(&path)?;
    let run_git = |args: &[&str]| {
        Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(args)
            .output()
            .map_err(|error| format!("Git is required to add a project: {error}"))
    };

    let repository = run_git(&["rev-parse", "--is-inside-work-tree"])?;
    if !repository.status.success()
        || !String::from_utf8_lossy(&repository.stdout).trim().eq_ignore_ascii_case("true")
    {
        let initialized = run_git(&["init", "--initial-branch=main"])?;
        if !initialized.status.success() {
            return Err(String::from_utf8_lossy(&initialized.stderr).trim().to_owned());
        }
    }

    let head = run_git(&["rev-parse", "--verify", "HEAD"])?;
    if !head.status.success() {
        let initial = Command::new("git")
            .arg("-C")
            .arg(directory)
            .args([
                "-c",
                "user.name=AgentOS",
                "-c",
                "user.email=agentos@localhost",
                "commit",
                "--allow-empty",
                "-m",
                "chore: initialize repository",
            ])
            .output()
            .map_err(|error| format!("Could not create the initial Git commit: {error}"))?;
        if !initial.status.success() {
            return Err(String::from_utf8_lossy(&initial.stderr).trim().to_owned());
        }
    }

    Ok(())
}

#[tauri::command]
fn reveal_project_folder(path: String) -> Result<(), String> {
    let directory = project_directory(&path)?;
    #[cfg(target_os = "windows")]
    let mut command = Command::new("explorer");
    #[cfg(target_os = "macos")]
    let mut command = Command::new("open");
    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = Command::new("xdg-open");

    command.arg(directory).spawn().map(|_| ()).map_err(|error| error.to_string())
}

#[tauri::command]
fn rename_project_folder(path: String, new_name: String) -> Result<String, String> {
    let directory = project_directory(&path)?;
    let parent = directory.parent().ok_or("Project folder has no parent directory.")?;
    let target = parent.join(folder_name(&new_name)?);
    if target.exists() {
        return Err("A folder with that name already exists.".into());
    }
    fs::rename(directory, &target).map_err(|error| error.to_string())?;
    Ok(target.to_string_lossy().into_owned())
}

#[tauri::command]
fn delete_project_folder(path: String) -> Result<(), String> {
    let directory = project_directory(&path)?;
    fs::remove_dir_all(directory).map_err(|error| error.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .invoke_handler(tauri::generate_handler![
            setup_project_repository,
            reveal_project_folder,
            rename_project_folder,
            delete_project_folder
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
