// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::{fs, io, str};

/// Runs a command, exiting the program immediately if the command fails
pub fn run_command_with_status_check(cmd_str: &str) -> io::Result<Output> {
    // we need to bind it to some arbitrary type that implements AsRef<Path>. I just chose &Path
    run_command_with_status_check_core::<&Path>(cmd_str, None)
}

/// Runs a command in a directory, exiting the program immediately if the command fails
pub fn run_command_with_status_check_in_dir<P: AsRef<Path>>(
    cmd_str: &str,
    in_path: P,
) -> io::Result<Output> {
    run_command_with_status_check_core::<P>(cmd_str, Some(in_path))
}

/// This function exposes all the different ways to run a command, but the interface is not
/// ergonomic. The ergonomic wrappers above are a workaround for Rust not having default values on
/// parameters.
pub fn run_command_with_status_check_core<P: AsRef<Path>>(
    cmd_str: &str,
    in_path: Option<P>,
) -> io::Result<Output> {
    // use shlex::split() instead of split_whitespace() to handle cases like quotes and escape chars
    let mut cmd_components: Vec<String> = shlex::split(cmd_str).unwrap();
    let cmd_name = cmd_components.remove(0);
    let args = cmd_components;
    let mut cmd = Command::new(cmd_name);
    cmd.args(args);
    if let Some(in_path) = in_path {
        cmd.current_dir(in_path);
    }
    let output = cmd.output()?;

    if output.status.success() {
        Ok(output)
    } else {
        Err(io::Error::other(
            format!(
                "```{}``` failed with ```{}```",
                cmd_str,
                String::from_utf8_lossy(&output.stderr)
            )
            .as_str(),
        ))
    }
}

/// Make dpath an existent but empty directory.
pub fn make_into_empty_dir<P>(dpath: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    if dpath.as_ref().exists() {
        fs::remove_dir_all(&dpath)?;
    }
    if !dpath.as_ref().exists() {
        fs::create_dir(&dpath)?;
    }
    Ok(())
}

/// Get the path of the root "optd" repo directory
pub fn get_optd_root() -> io::Result<PathBuf> {
    get_optd_root_from(&std::env::current_dir()?)
}

fn get_optd_root_from(directory: &Path) -> io::Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(directory)
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other("not inside an optd Git worktree"));
    }
    let root = PathBuf::from(
        str::from_utf8(&output.stdout)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .trim(),
    );
    if ![
        "Cargo.toml",
        "optd-core/Cargo.toml",
        "optd-perfbench/Cargo.toml",
    ]
    .iter()
    .all(|file| root.join(file).is_file())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Git worktree is not the optd workspace",
        ));
    }
    Ok(root)
}

/// Can be an absolute path or a relative path. Regardless of where this CLI is run, relative paths
/// are evaluated relative to the optd repo root.
pub fn parse_pathstr(pathstr: &str) -> io::Result<PathBuf> {
    let path = PathBuf::from(pathstr);
    let path = if path.is_relative() {
        get_optd_root()?.join(path)
    } else {
        path
    };
    Ok(path)
}

/// Get a repo to its latest state by either cloning or pulling
pub fn clonepull_repo<P: AsRef<Path>>(repo_url: &str, repo_dpath: P) -> io::Result<()> {
    if !repo_dpath.as_ref().exists() {
        log::debug!("[start] cloning {} repo", repo_url);
        run_command_with_status_check(&format!(
            "git clone {} {}",
            repo_url,
            repo_dpath.as_ref().to_str().unwrap()
        ))?;
        log::debug!("[end] cloning {} repo", repo_url);
    } else {
        log::debug!("[skip] cloning {} repo", repo_url);
    }
    log::debug!("[start] pulling latest {} repo", repo_url);
    run_command_with_status_check_in_dir("git pull", &repo_dpath)?;
    log::debug!("[end] pulling latest {} repo", repo_url);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn workspace_root_accepts_forks_missing_origins_and_nested_directories() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("optd-root-{}-{nonce}", std::process::id()));
        fs::create_dir(&dir).unwrap();
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(dir.clone());
        assert!(Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(&dir)
            .status()
            .unwrap()
            .success());
        assert!(get_optd_root_from(&dir).is_err());
        for file in [
            "Cargo.toml",
            "optd-core/Cargo.toml",
            "optd-perfbench/Cargo.toml",
        ] {
            let path = dir.join(file);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, "").unwrap();
        }
        let expected = fs::canonicalize(&dir).unwrap();
        assert_eq!(
            get_optd_root_from(&dir.join("optd-core")).unwrap(),
            expected
        );
        assert!(Command::new("git")
            .args([
                "remote",
                "add",
                "origin",
                "https://example.com/fork/optd.git"
            ])
            .current_dir(&dir)
            .status()
            .unwrap()
            .success());
        assert_eq!(get_optd_root_from(&dir).unwrap(), expected);
    }
}
