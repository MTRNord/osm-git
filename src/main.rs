use std::{fs::File, time::Duration};

use clap::Parser;
use color_eyre::eyre::Result;
use git2::Signature;
use memmap2::Mmap;
use tracing::{info, warn};

use crate::{git::init_git_repository, osm::osm_data::convert_objects_to_git};

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
    /// Where to write cache files
    #[arg(long, default_value = "./cache")]
    cache_path: String,
    /// If the git repo should be removed and recreated
    #[arg(short, long)]
    clean: bool,
    /// Where to start downloading data from
    #[arg(long, default_value = "000/000/000")]
    start_data: String,
    /// The time to wait between downloading data
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

    let repository = init_git_repository(&cli.git_repo_path, &cli.replication_server, &author)?;
    info!("Git repository initialized");

    // Data download metadata
    // TODO: We should probably detect where to resume from
    let mut data_position_top = cli.start_data[0..3].parse::<u16>()?;
    let mut data_position_middle = cli.start_data[4..7].parse::<u16>()?;
    let mut data_position_bottom = cli.start_data[8..11].parse::<u16>()?;

    // Parse the changesets and convert them to git objects
    loop {
        // Check for cache and use it if it exists
        let cache_file_path = format!(
            "{}/replication/{:03}/{:03}/{:03}.osm.gz",
            cli.cache_path, data_position_top, data_position_middle, data_position_bottom
        );

        if std::path::Path::new(&cache_file_path).exists() {
            info!("Using cached data file at {}", cache_file_path);
            let file = File::open(&cache_file_path)?;
            let data = unsafe { Mmap::map(&file)? };
            let changeset_location = format!("{}/changesets/torrents", cli.cache_path);
            convert_objects_to_git(&repository, &author, &data, &changeset_location)?;
            info!("Data file parsed");

            // Increment the data position
            if data_position_top == 999
                && data_position_middle == 999
                && data_position_bottom == 999
            {
                // Uhhhhhh?!
                break;
            }

            if data_position_middle == 999 && data_position_bottom == 999 {
                data_position_middle = 0;
                data_position_bottom = 0;
                data_position_top += 1;
            }

            if data_position_bottom == 999 {
                data_position_bottom = 0;
                data_position_middle += 1;
            }
        } else {
            {
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
                    // Increment the data position
                    if data_position_top == 999
                        && data_position_middle == 999
                        && data_position_bottom == 999
                    {
                        // Uhhhhhh?!
                        break;
                    }

                    if data_position_middle == 999 && data_position_bottom == 999 {
                        data_position_middle = 0;
                        data_position_bottom = 0;
                        data_position_top += 1;
                    }

                    if data_position_bottom == 999 {
                        data_position_bottom = 0;
                        data_position_middle += 1;
                    }

                    if data_position_bottom < 999 {
                        data_position_bottom += 1;
                    }

                    continue;
                }

                let data = data_response.bytes().await?;
                info!("Caching Data file to disk");
                std::fs::create_dir_all(std::path::Path::new(&cache_file_path).parent().unwrap())?;
                std::fs::write(&cache_file_path, &data)?;
                info!("Data file downloaded");
            };

            let file = File::open(cache_file_path)?;
            let data = unsafe { Mmap::map(&file)? };

            let changeset_location = format!("{}/changesets/torrents", cli.cache_path);
            convert_objects_to_git(&repository, &author, &data, &changeset_location)?;

            // Increment the data position
            if data_position_top == 999
                && data_position_middle == 999
                && data_position_bottom == 999
            {
                // Uhhhhhh?!
                break;
            }

            if data_position_middle == 999 && data_position_bottom == 999 {
                data_position_middle = 0;
                data_position_bottom = 0;
                data_position_top += 1;
            }

            if data_position_bottom == 999 {
                data_position_bottom = 0;
                data_position_middle += 1;
            }

            if data_position_bottom < 999 {
                data_position_bottom += 1;
            }

            // Wait a few seconds before downloading the next data file
            tokio::time::sleep(Duration::from_millis(cli.wait_time)).await;
        }
    }

    info!(
        "Downloaded data until {} {} {}",
        data_position_top,
        data_position_middle,
        data_position_bottom - 1
    );

    Ok(())
}
