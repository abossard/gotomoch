use anyhow::{Context, Result};
use async_compression::tokio::bufread::GzipDecoder;
use chrono::{NaiveDate, Utc};
use clap::{Parser, Subcommand};
use gotomoch::{
    azure::Azure,
    config::Config,
    pipeline::{CsvSource, Cursor, Input},
    runner, state,
};
use std::{path::PathBuf, process::ExitCode};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about = "Incremental Azure cost aggregation and delivery")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Migrate unread days, provisioning missing resources when enabled.
    Run {
        #[arg(short, long, default_value = "gotomoch.toml")]
        config: PathBuf,
        /// Inclusive last day. Defaults to UTC today minus job.lag_days.
        #[arg(long)]
        until: Option<NaiveDate>,
    },
    /// Show the persisted state and lease without changing resources.
    Status {
        #[arg(short, long, default_value = "gotomoch.toml")]
        config: PathBuf,
    },
    /// Aggregate a local CSV/CSV.gz to JSON Lines without Azure access.
    Preview {
        #[arg(short, long, default_value = "gotomoch.toml")]
        config: PathBuf,
        #[arg(long)]
        input: PathBuf,
    },
    /// Validate configuration without Azure access.
    Check {
        #[arg(short, long, default_value = "gotomoch.toml")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match execute().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn execute() -> Result<()> {
    let filter = match std::env::var("RUST_LOG") {
        Ok(value) => tracing_subscriber::EnvFilter::try_new(value)?,
        Err(std::env::VarError::NotPresent) => tracing_subscriber::EnvFilter::new("info"),
        Err(error) => return Err(error).context("reading RUST_LOG"),
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
    let command = Cli::parse().command;
    let path = match &command {
        Command::Run { config, .. }
        | Command::Status { config }
        | Command::Preview { config, .. }
        | Command::Check { config } => config,
    };
    let config = Config::load(path).await?;
    match command {
        Command::Check { .. } => println!("Configuration is valid."),
        Command::Preview { input, .. } => {
            let file = tokio::fs::File::open(&input)
                .await
                .with_context(|| format!("opening {}", input.display()))?;
            let input: Input = if input.extension().is_some_and(|e| e == "gz") {
                Box::pin(GzipDecoder::new(BufReader::new(file)))
            } else {
                Box::pin(file)
            };
            let mut source = CsvSource::new(input, Cursor::default(), &config).await?;
            let observed = Utc::now();
            let mut output = tokio::io::stdout();
            loop {
                let chunk = source.read_chunk(&config, None).await?;
                for record in chunk.records(&config, 0, observed)? {
                    output.write_all(&serde_json::to_vec(&record)?).await?;
                    output.write_all(b"\n").await?;
                }
                if chunk.eof {
                    break;
                }
            }
            output.flush().await?;
        }
        Command::Status { .. } => {
            let azure = Azure::new(CancellationToken::new())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state::status(&azure, &config).await?)?
            );
        }
        Command::Run { until, .. } => {
            let until = match until {
                Some(day) => day,
                None => Utc::now()
                    .date_naive()
                    .checked_sub_signed(chrono::Duration::days(config.job.lag_days.into()))
                    .context("job.lag_days is outside the supported date range")?,
            };
            let cancellation = CancellationToken::new();
            let azure = Azure::new(cancellation.clone())?;
            let signal = tokio::spawn(async move {
                match tokio::signal::ctrl_c().await {
                    Ok(()) => cancellation.cancel(),
                    Err(error) => {
                        tracing::error!(%error, "could not register Ctrl-C handler");
                        cancellation.cancel();
                    }
                }
            });
            let result = runner::run(&azure, &config, until).await;
            signal.abort();
            println!("{}", serde_json::to_string_pretty(&result?)?);
        }
    }
    Ok(())
}
