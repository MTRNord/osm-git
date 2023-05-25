use std::{io::Write, path::Path};

use color_eyre::eyre::Result;
use gix::{actor::Signature, bstr::BString, create, objs::tree, ThreadSafeRepository};
use tracing::{info, warn};

/// Initialize the git repository
///
/// If the git repository already exists, open it. Otherwise, create it.
///
/// If the git repository is created, generate the README.md file from the template.
///
/// # Arguments
///
/// * `git_repo_path` - The path to the git repository
/// * `data_url` - The URL to the OSM data server
/// * `changeset_url` - The URL to the OSM changeset server
///
/// # Returns
///
/// * `Result<Repository>` - The git repository
pub fn init_git_repository(
    git_repo_path: &str,
    data_url: &str,
    author: &Signature,
) -> Result<ThreadSafeRepository> {
    // Check if the git repo already exists
    if std::path::Path::new(git_repo_path).exists() {
        info!("Git repository already exists at {}", git_repo_path);
        // Open the git repo
        let repository = ThreadSafeRepository::open(git_repo_path)?;

        return Ok(repository);
    }

    info!("Initializing git repository at {}", git_repo_path);

    // Create the git repo if it doesn't exist
    let repository = ThreadSafeRepository::init(
        git_repo_path,
        create::Kind::WithWorktree,
        create::Options::default(),
    )?;

    let readme_filepath = generate_readme_from_template(&repository, data_url)?;

    // Commit the README.md file
    commit(
        &repository,
        vec![readme_filepath],
        vec![],
        "Create the README.md",
        author,
        author,
    )?;
    Ok(repository)
}

/// Generate the README.md file from the template and write it to the git repo
pub fn generate_readme_from_template(
    repository: &ThreadSafeRepository,
    data_url: &str,
) -> Result<String> {
    let template_file = include_str!("../../templates/README.md");

    // Replace the template variables with the actual values
    let template_file = template_file.replace("$server_url", data_url);

    // Get the version of this binary
    let version = env!("CARGO_PKG_VERSION");
    let template_file = template_file.replace("$version", version);

    // Write the README.md file in the git repo (parent of .git directory)
    let path = repository
        .path()
        .parent()
        .expect("Git repository path is not valid");
    let readme_file_path = path.join("README.md");
    info!(
        "Generating README.md file at {}",
        readme_file_path
            .to_str()
            .expect("README.md file path is not valid")
    );
    let mut readme_file = std::fs::File::create(&readme_file_path)?;
    readme_file.write_all(template_file.as_bytes())?;
    readme_file.sync_all()?;

    info!("README.md file generated");

    Ok(readme_file_path
        .to_str()
        .expect("README.md file path is not valid")
        .to_string())
}

/// Helper for creating a git commit
pub fn commit(
    repository: &ThreadSafeRepository,
    added_or_changed_files: Vec<String>,
    removed_files: Vec<String>,
    message: &str,
    author: &Signature,
    committer: &Signature,
) -> Result<()> {
    let mut repository = repository.to_thread_local();

    let mut tree = {
        let head_commit = repository.head_commit();
        if let Ok(ref head_commit) = head_commit {
            let tree = head_commit.tree()?;
            tree.decode()?.into()
        } else {
            gix::objs::Tree::empty()
        }
    };
    let workdir = { repository.work_dir().unwrap().to_owned() };

    let config = repository.config_snapshot_mut();
    {
        let repository = config.commit_auto_rollback()?;
        for file in added_or_changed_files {
            let file_path = Path::new(&file);

            // TODO: I am tired to actually debug this so we just do a sanity check if the file exists
            if file_path.exists() {
                // Load file to bytes
                let file = std::fs::File::open(file_path)?;
                let relative_path = if file_path.starts_with(workdir.clone()) {
                    Path::new(&file_path).strip_prefix(workdir.clone())?
                } else {
                    Path::new(&file_path)
                };

                let blob_id = repository.write_blob_stream(file)?.into();
                let entry = tree::Entry {
                    mode: tree::EntryMode::Blob,
                    oid: blob_id,
                    filename: relative_path.to_str().unwrap().into(),
                };

                tree.entries.push(entry);
            } else {
                warn!(
                    "File {} does not exist but was meant to be added",
                    file_path.to_str().unwrap()
                );
            }
        }
        for file in removed_files {
            let file_path = Path::new(&file);
            // We check if it was tracked before. If not we don't need to remove it
            let relative_path = if file_path.starts_with(workdir.clone()) {
                Path::new(&file_path).strip_prefix(workdir.clone())?
            } else {
                Path::new(&file_path)
            };
            let relative_filename: BString = relative_path.to_str().unwrap().into();
            tree.entries.retain(|e| e.filename != relative_filename);
        }

        let tree_id = repository.write_object(&tree)?;

        {
            let head_commit = repository.head_commit();
            if let Ok(head_commit) = head_commit {
                repository.commit_as(
                    committer,
                    author,
                    "HEAD",
                    message,
                    tree_id,
                    [head_commit.id],
                )?;
            } else {
                repository.commit_as(
                    committer,
                    author,
                    "HEAD",
                    message,
                    tree_id,
                    gix::commit::NO_PARENT_IDS,
                )?;
            }
        }
    }
    Ok(())
}
