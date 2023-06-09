use std::{io::Write, path::Path};

use color_eyre::eyre::Result;
use git2::{Oid, Repository, Signature};
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
) -> Result<Repository> {
    // Check if the git repo already exists
    if std::path::Path::new(git_repo_path).exists() {
        info!("Git repository already exists at {}", git_repo_path);
        // Open the git repo
        let repository = Repository::open(git_repo_path)?;

        return Ok(repository);
    }

    info!("Initializing git repository at {}", git_repo_path);

    // Create the git repo if it doesn't exist
    let repository = Repository::init(git_repo_path)?;

    generate_readme_from_template(&repository, data_url)?;

    // Commit the README.md file
    commit(
        &repository,
        vec!["README.md".to_string()],
        vec![],
        "Create the README.md",
        author,
        author,
    )?;
    Ok(repository)
}

/// Generate the README.md file from the template and write it to the git repo
pub fn generate_readme_from_template(repository: &Repository, data_url: &str) -> Result<()> {
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
    let mut readme_file = std::fs::File::create(readme_file_path)?;
    readme_file.write_all(template_file.as_bytes())?;
    readme_file.sync_all()?;

    info!("README.md file generated");

    Ok(())
}

/// Helper for creating a git commit
pub fn commit(
    repository: &Repository,
    added_or_changed_files: Vec<String>,
    removed_files: Vec<String>,
    message: &str,
    author: &Signature,
    committer: &Signature,
) -> Result<Oid> {
    let tree_id = {
        let mut index = repository.index()?;
        for file in added_or_changed_files {
            let file_path = Path::new(&file);
            let path = if file_path.starts_with(repository.path().parent().unwrap()) {
                Path::new(&file).strip_prefix(repository.path().parent().unwrap())?
            } else {
                Path::new(&file)
            };
            // TODO: I am tired to actually debug this so we just do a sanity check if the file exists
            if file_path.exists() {
                index.add_path(path)?;
            } else {
                warn!(
                    "File {} does not exist but was meant to be added",
                    path.to_str().unwrap()
                );
            }
        }
        for file in removed_files {
            let file_path = Path::new(&file);
            let path = if file_path.starts_with(repository.path().parent().unwrap()) {
                Path::new(&file).strip_prefix(repository.path().parent().unwrap())?
            } else {
                Path::new(&file)
            };
            // We check if it was tracked before. If not we don't need to remove it
            if index.get_path(path, 0).is_some() {
                index.remove_path(path)?;
            }
        }
        index.write()?;
        index.write_tree()?
    };
    let tree = repository.find_tree(tree_id)?;
    let head_id = repository.refname_to_id("HEAD");
    if let Ok(head_id) = head_id {
        let parent = repository.find_commit(head_id)?;

        let oid = repository.commit(Some("HEAD"), author, committer, message, &tree, &[&parent])?;
        Ok(oid)
    } else {
        let oid = repository.commit(Some("HEAD"), author, committer, message, &tree, &[])?;
        Ok(oid)
    }
}
