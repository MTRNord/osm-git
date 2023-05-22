use std::time::Duration;

use clap::Parser;
use color_eyre::eyre::Result;
use git2::Signature;
use tracing::{info, warn};

use crate::{
    git::init_git_repository,
    osm::{changesets::parse_changeset, osm_data::convert_objects_to_git},
};

mod git;
mod osm;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the git repo to replay changesets to
    #[arg(short, long, default_value = "./osm-git-repo")]
    git_repo_path: String,
    /// The server to get day replication files from
    #[arg(
        short,
        long,
        default_value = "https://planet.openstreetmap.org/replication/day"
    )]
    replication_server: String,
    /// The server to get changeset data from
    #[arg(
        long,
        default_value = "https://planet.openstreetmap.org/replication/changesets"
    )]
    changeset_server: String,
    /// Where to write cache files
    #[arg(long, default_value = "./cache")]
    cache_path: String,
    /// If the git repo should be removed and recreated
    #[arg(short, long)]
    clean: bool,
    /// Where to start downloading changesets from
    #[arg(long, default_value = "000/000/000")]
    start_changesets: String,
    /// The time to wait between downloading changesets
    /// This is to avoid causing a lot of load on the OSM servers
    #[arg(long, default_value = "500")]
    wait_time: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    info!(
        "Starting to replay osm changesets to git repo at {}",
        cli.git_repo_path
    );

    let client = reqwest::Client::builder()
        .user_agent("osm-git-replay/0.1.0")
        .gzip(true)
        .timeout(Duration::from_secs(60))
        .build()?;

    if cli.clean {
        info!("Cleaning git repo at {}", cli.git_repo_path);
        if std::path::Path::new(&cli.git_repo_path).exists() {
            std::fs::remove_dir_all(&cli.git_repo_path)?;
        }
    }

    let author = Signature::now("osm-git-replay", "osm-git-replay@localhost")?;

    let repository = init_git_repository(
        &cli.git_repo_path,
        &cli.replication_server,
        &author,
        &cli.changeset_server,
    )?;
    info!("Git repository initialized");

    // Main download loop
    let mut changeset_position_top = cli.start_changesets[0..3].parse::<u64>()?;
    let mut changeset_position_middle = cli.start_changesets[4..7].parse::<u64>()?;
    let mut changeset_position_bottom = cli.start_changesets[8..11].parse::<u64>()?;
    let mut changeset_position_middle_incremented = false;
    let mut changeset_position_top_incremented = false;

    let mut changesets = Vec::new();
    loop {
        // Check for cache and use it if it exists
        let cache_file_path = format!(
            "{}/changesets/{:03}/{:03}/{:03}.osm.gz",
            cli.cache_path,
            changeset_position_top,
            changeset_position_middle,
            changeset_position_bottom
        );

        if std::path::Path::new(&cache_file_path).exists() {
            info!("Using cached changeset file at {}", cache_file_path);
            let changeset_data = std::fs::read(&cache_file_path)?;
            let parsed_changeset = parse_changeset(changeset_data.into())?;
            info!("Changeset file parsed");
            changesets.extend(parsed_changeset);

            // Increment the changeset position
            changeset_position_bottom += 1;
            changeset_position_middle_incremented = false;
            changeset_position_top_incremented = false;
        } else {
            // First we download the changeset files
            let changeset_url = format!(
                "{}/{:03}/{:03}/{:03}.osm.gz",
                cli.changeset_server,
                changeset_position_top,
                changeset_position_middle,
                changeset_position_bottom
            );
            info!("Downloading changeset file from {}", changeset_url);
            let changeset_response: reqwest::Response = client.get(&changeset_url).send().await?;
            if changeset_response.status() == reqwest::StatusCode::NOT_FOUND {
                warn!("Changeset file not found at {}", changeset_url);
                // We've reached the end of the changesets for this bottom position.
                // If we incremented top and failed again, we're done.
                if changeset_position_top_incremented {
                    info!("Finished or failed downloading changesets");
                    info!(
                        "Changeset position: {} {} {}",
                        changeset_position_top,
                        changeset_position_middle,
                        changeset_position_bottom
                    );
                    warn!("Response body: {:?}", changeset_response.text().await?);
                    // TODO: We want to have an endless loop here optionally so that we can keep trying to download changesets.
                    break;
                }
                // We reset bottom to 0 and increment middle.
                // We also mark middle as incremented so that we increment top on the next failure.
                if !changeset_position_middle_incremented && changeset_position_bottom != 0 {
                    changeset_position_bottom = 0;
                    changeset_position_middle += 1;
                    changeset_position_middle_incremented = true;
                    changeset_position_top_incremented = false;
                } else {
                    changeset_position_middle_incremented = false;
                    changeset_position_top += 1;
                    changeset_position_top_incremented = true;
                    changeset_position_middle = 0;
                    changeset_position_bottom = 0;
                }
                continue;
            }
            let changeset_data = changeset_response.bytes().await?;
            info!("Caching changeset file to disk");
            std::fs::create_dir_all(std::path::Path::new(&cache_file_path).parent().unwrap())?;
            std::fs::write(&cache_file_path, &changeset_data)?;
            info!("Changeset file downloaded");

            let parsed_changeset = parse_changeset(changeset_data)?;
            info!("Changeset file parsed");
            changesets.extend(parsed_changeset);

            // Increment the changeset position
            changeset_position_bottom += 1;
            changeset_position_middle_incremented = false;
            changeset_position_top_incremented = false;

            // Wait a few seconds before downloading the next changeset file
            tokio::time::sleep(Duration::from_millis(cli.wait_time)).await;
        }
    }

    // Data download metadata
    let mut data_position_top = 0;
    let mut data_position_middle = 0;
    let mut data_position_bottom = 0;
    let mut data_position_middle_incremented = false;
    let mut data_position_top_incremented = false;

    // Parse the changesets and convert them to git objects
    loop {
        // Check for cache and use it if it exists
        let cache_file_path = format!(
            "{}/replication/{:03}/{:03}/{:03}.osm.gz",
            cli.cache_path,
            changeset_position_top,
            changeset_position_middle,
            changeset_position_bottom
        );

        if std::path::Path::new(&cache_file_path).exists() {
            info!("Using cached data file at {}", cache_file_path);
            let data = std::fs::read(&cache_file_path)?;
            convert_objects_to_git(&repository, &author, &changesets, &data)?;
            info!("Data file parsed");

            // Increment the data position
            data_position_bottom += 1;
            data_position_middle_incremented = false;
            data_position_top_incremented = false;
        } else {
            // Download minute replication files and find the changesets that were modified in that minute
            let data_url = format!(
                "{}/{:03}/{:03}/{:03}.osc.gz",
                cli.replication_server,
                data_position_top,
                data_position_middle,
                data_position_bottom
            );
            info!("Downloading data file from {}", data_url);
            let data_response: reqwest::Response = client.get(&data_url).send().await?;

            if data_response.status() == reqwest::StatusCode::NOT_FOUND {
                warn!("data file not found at {}", data_url);
                // We've reached the end of the data for this bottom position.
                // If we incremented top and failed again, we're done.
                if data_position_top_incremented {
                    info!("Finished or failed downloading data");
                    info!(
                        "Data position: {} {} {}",
                        data_position_top, data_position_middle, data_position_bottom
                    );
                    warn!("Response body: {:?}", data_response.text().await?);

                    // TODO: We want to have an endless loop here optionally so that we can keep trying to download changesets.
                    break;
                }
                // We reset bottom to 0 and increment middle.
                // We also mark middle as incremented so that we increment top on the next failure.
                if !data_position_middle_incremented && data_position_bottom != 0 {
                    data_position_bottom = 0;
                    data_position_middle += 1;
                    data_position_middle_incremented = true;
                    data_position_top_incremented = false;
                } else {
                    data_position_middle_incremented = false;
                    data_position_top += 1;
                    data_position_top_incremented = true;
                    data_position_middle = 0;
                    data_position_bottom = 0;
                }
                continue;
            }

            let data = data_response.bytes().await?;
            info!("Caching Data file to disk");
            std::fs::create_dir_all(std::path::Path::new(&cache_file_path).parent().unwrap())?;
            std::fs::write(&cache_file_path, &data)?;
            info!("Data file downloaded");
            convert_objects_to_git(&repository, &author, &changesets, &data)?;

            // Increment the data position
            data_position_bottom += 1;
            data_position_middle_incremented = false;
            data_position_top_incremented = false;

            // Wait a few seconds before downloading the next data file
            tokio::time::sleep(Duration::from_millis(cli.wait_time)).await;
        }
    }

    info!(
        "Downloaded changesets until {} {} {}",
        changeset_position_top,
        changeset_position_middle,
        changeset_position_bottom - 1
    );
    info!(
        "Downloaded data until {} {} {}",
        data_position_top,
        data_position_middle,
        data_position_bottom - 1
    );

    Ok(())
}
