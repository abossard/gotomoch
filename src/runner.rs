use crate::{
    azure::{Azure, Blob, Report, ReportPoll, ReportStart, Sink},
    config::Config,
    pipeline::{Batch, Chunk, CsvSource},
    state::{Event, Lease, Pending, Phase, State},
};
use anyhow::{Context, Result, bail, ensure};
use chrono::{Months, NaiveDate, Utc};
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde_json::{Value, json};
use std::{collections::BTreeSet, time::Duration};

struct OpenSource {
    operation: String,
    blob: usize,
    csv: CsvSource,
}

struct Prepared {
    pending: Pending,
    batches: Vec<Batch>,
}

pub async fn run(azure: &Azure, config: &Config, until: NaiveDate) -> Result<Value> {
    config.validate()?;
    ensure!(
        until < Utc::now().date_naive(),
        "--until must be before today; cost data for an open day is incomplete"
    );
    azure
        .ensure_state_resources(config)
        .await
        .context("preparing checkpoint storage")?;
    let lease = Lease::acquire(azure, config).await?;
    let result = run_owned(azure, config, until, &lease).await;
    let release = lease.finish().await;
    match (result, release) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(release)) => {
            Err(error).context(format!("also unable to release the job lease: {release:#}"))
        }
    }
}

async fn run_owned(
    azure: &Azure,
    config: &Config,
    until: NaiveDate,
    lease: &Lease,
) -> Result<Value> {
    let mut state = lease.state().await?;
    if state.day > until {
        return Ok(summary(config, until, &state));
    }
    let earliest = Utc::now()
        .date_naive()
        .checked_sub_months(Months::new(13))
        .context("date underflow")?;
    let mut sink = None;
    let mut report: Option<(String, Report)> = None;
    let mut source: Option<OpenSource> = None;
    let mut prepared: Option<Prepared> = None;
    while state.day <= until {
        lease.ensure_owned().await?;
        ensure!(!azure.cancellation.is_cancelled(), "migration cancelled");
        match state.phase.clone() {
            Phase::Ready => {
                ensure!(
                    state.day >= earliest,
                    "next unread day {} is older than the Cost Details API's 13-month history limit",
                    state.day
                );
                tracing::info!(day = %state.day, "requesting the next unread cost day");
                match azure.start_report(config, state.day).await? {
                    ReportStart::Empty => state = lease.apply(Event::EmptyDay).await?,
                    ReportStart::Pending { operation, delay } => {
                        state = lease.apply(Event::ReportRequested(operation)).await?;
                        azure.sleep(delay).await?;
                    }
                    ReportStart::Ready {
                        operation,
                        report: completed,
                    } => {
                        state = lease
                            .apply(Event::ReportRequested(operation.clone()))
                            .await?;
                        report = Some((operation, completed));
                    }
                }
            }
            Phase::AwaitingReport { operation } => {
                if report
                    .as_ref()
                    .is_none_or(|(cached, _)| cached != &operation)
                {
                    report = wait_report(azure, &operation)
                        .await?
                        .map(|report| (operation, report));
                }
                state = lease
                    .apply(if let Some((_, report)) = &report {
                        Event::ReportReady(report.blobs.len())
                    } else {
                        Event::EmptyDay
                    })
                    .await?;
            }
            Phase::Reading {
                operation,
                blob,
                blobs,
                cursor,
            } => {
                if blob == blobs {
                    state = lease.apply(Event::DayFinished).await?;
                    report = None;
                    source = None;
                    continue;
                }
                ensure!(
                    blob < blobs,
                    "checkpoint blob index exceeds its report manifest"
                );
                let manifest = current_report(azure, &operation, &mut report).await?;
                ensure!(
                    manifest.blobs.len() == blobs,
                    "cost report blob count changed since checkpoint"
                );
                let source_blob = &manifest.blobs[blob];
                if !source_blob.is_gzip(manifest.compressed)? {
                    ensure!(
                        cursor.offset <= source_blob.byte_count,
                        "saved CSV offset exceeds the report blob length"
                    );
                    if cursor.offset == source_blob.byte_count && cursor.offset > 0 {
                        state = lease.apply(Event::BlobFinished).await?;
                        source = None;
                        continue;
                    }
                }
                if source.as_ref().is_none_or(|s| {
                    s.operation != operation || s.blob != blob || s.csv.cursor() != &cursor
                }) {
                    let csv = azure
                        .open_blob(
                            &manifest.blobs[blob],
                            manifest.compressed,
                            cursor.clone(),
                            config,
                        )
                        .await?;
                    source = Some(OpenSource {
                        operation,
                        blob,
                        csv,
                    });
                }
                let input = &mut source.as_mut().context("source was not opened")?.csv;
                if cursor.offset == 0 {
                    state = lease
                        .apply(Event::SourceOpened(input.cursor().clone()))
                        .await?;
                    continue;
                }
                let chunk = tokio::select! {
                    _ = azure.cancellation.cancelled() => bail!("migration cancelled"),
                    chunk = input.read_chunk(config, Some(state.day)) => chunk?,
                };
                let chunk = check_source_end(chunk, source_blob, manifest.compressed)?;
                if chunk.input_rows == 0 {
                    ensure!(chunk.eof, "CSV reader produced an empty nonterminal chunk");
                    state = lease.apply(Event::BlobFinished).await?;
                } else {
                    let next = prepare(chunk, config, blob, Utc::now())?;
                    state = lease
                        .apply(Event::ChunkPrepared(next.pending.clone()))
                        .await?;
                    prepared = Some(next);
                }
            }
            Phase::Writing {
                operation,
                blob,
                pending,
                ..
            } => {
                if pending.completed.len() == pending.batch_count {
                    state = lease.apply(Event::ChunkCommitted).await?;
                    prepared = None;
                    continue;
                }
                if prepared
                    .as_ref()
                    .is_none_or(|p| !same_chunk(&p.pending, &pending))
                {
                    let manifest = current_report(azure, &operation, &mut report).await?;
                    let source_blob = manifest
                        .blobs
                        .get(blob)
                        .context("saved source blob is missing from the report")?;
                    let mut csv = azure
                        .open_blob(
                            source_blob,
                            manifest.compressed,
                            pending.start.clone(),
                            config,
                        )
                        .await?;
                    let chunk = tokio::select! {
                        _ = azure.cancellation.cancelled() => bail!("migration cancelled"),
                        chunk = csv.read_chunk(config, Some(state.day)) => chunk?,
                    };
                    let chunk = check_source_end(chunk, source_blob, manifest.compressed)?;
                    let replay = prepare(chunk, config, blob, pending.observed_at)?;
                    ensure!(
                        same_chunk(&replay.pending, &pending),
                        "source chunk or encoded batches changed since checkpoint; refusing an unsafe replay"
                    );
                    prepared = Some(replay);
                    source = Some(OpenSource {
                        operation,
                        blob,
                        csv,
                    });
                }
                if sink.is_none() {
                    sink = Some(
                        azure
                            .ensure_target(config)
                            .await
                            .context("preparing ingestion resources")?,
                    );
                }
                write_pending(
                    azure,
                    config,
                    sink.as_ref()
                        .context("ingestion target was not initialized")?,
                    lease,
                    &pending,
                    &prepared.as_ref().context("chunk was not prepared")?.batches,
                )
                .await?;
                state = lease.apply(Event::ChunkCommitted).await?;
                tracing::info!(day = %state.day, input_rows = pending.input_rows, batches = pending.batch_count,
                    "committed cost chunk");
                prepared = None;
            }
        }
    }
    Ok(summary(config, until, &state))
}

fn prepare(
    chunk: Chunk,
    config: &Config,
    blob: usize,
    observed_at: chrono::DateTime<Utc>,
) -> Result<Prepared> {
    let batches = chunk.batches(config, blob, observed_at)?;
    Ok(Prepared {
        pending: Pending {
            start: chunk.start,
            end: chunk.end,
            input_rows: chunk.input_rows,
            eof: chunk.eof,
            digest: chunk.digest,
            observed_at,
            batch_count: batches.len(),
            completed: BTreeSet::new(),
        },
        batches,
    })
}

fn check_source_end(mut chunk: Chunk, blob: &Blob, compressed: bool) -> Result<Chunk> {
    if !blob.is_gzip(compressed)? {
        ensure!(
            chunk.end.offset <= blob.byte_count,
            "cost report exceeds its advertised byte count"
        );
        ensure!(
            !chunk.eof || chunk.end.offset == blob.byte_count,
            "cost report blob ended before its advertised byte count"
        );
        chunk.eof |= chunk.end.offset == blob.byte_count;
    }
    Ok(chunk)
}

fn same_chunk(left: &Pending, right: &Pending) -> bool {
    left.start == right.start
        && left.end == right.end
        && left.input_rows == right.input_rows
        && left.eof == right.eof
        && left.digest == right.digest
        && left.observed_at == right.observed_at
        && left.batch_count == right.batch_count
}

async fn write_pending(
    azure: &Azure,
    config: &Config,
    sink: &Sink,
    lease: &Lease,
    pending: &Pending,
    batches: &[Batch],
) -> Result<()> {
    let mut remaining = batches
        .iter()
        .enumerate()
        .filter(|(index, _)| !pending.completed.contains(index));
    let mut in_flight = FuturesUnordered::new();
    loop {
        while in_flight.len() < config.limits.concurrency {
            let Some((index, batch)) = remaining.next() else {
                break;
            };
            in_flight.push(async move {
                lease.ensure_owned().await?;
                azure.send_batch(sink, batch).await?;
                Ok::<_, anyhow::Error>((index, batch.records))
            });
        }
        let Some(result) = in_flight.next().await else {
            break;
        };
        let (index, records) = result?;
        lease.apply(Event::BatchAccepted { index, records }).await?;
    }
    Ok(())
}

async fn wait_report(azure: &Azure, operation: &str) -> Result<Option<Report>> {
    tokio::time::timeout(Duration::from_secs(3600), async {
        loop {
            match azure.poll_report(operation).await? {
                ReportPoll::Pending(delay) => azure.sleep(delay).await?,
                ReportPoll::Ready(report) => return Ok(Some(report)),
                ReportPoll::Empty => return Ok(None),
            }
        }
    }).await.context("cost report did not finish within one hour; its operation is checkpointed for the next run")?
}

async fn current_report<'a>(
    azure: &Azure,
    operation: &str,
    cached: &'a mut Option<(String, Report)>,
) -> Result<&'a Report> {
    if cached.as_ref().is_none_or(|(key, _)| key != operation) {
        let report = wait_report(azure, operation)
            .await?
            .context("a report that previously contained data now reports NoDataFound")?;
        *cached = Some((operation.to_owned(), report));
    }
    Ok(&cached.as_ref().context("report was not loaded")?.1)
}

fn summary(config: &Config, until: NaiveDate, state: &State) -> Value {
    json!({
        "job": config.job.id, "through": until, "next_date": state.day,
        "delivery": if matches!(config.target, crate::config::Target::Direct) { "logs-ingestion-api" } else { "event-hub" },
        "totals": state.totals,
    })
}
