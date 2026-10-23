use crate::pipeline::Cursor;
use anyhow::{Result, bail, ensure};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct State {
    pub version: u32,
    pub fingerprint: String,
    pub day: NaiveDate,
    pub phase: Phase,
    pub totals: Totals,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Totals {
    pub input_rows: u64,
    pub output_rows: u64,
    pub accepted_batches: u64,
    pub completed_days: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "name", rename_all = "snake_case")]
pub enum Phase {
    Ready,
    AwaitingReport {
        operation: String,
    },
    Reading {
        operation: String,
        blob: usize,
        blobs: usize,
        cursor: Cursor,
    },
    Writing {
        operation: String,
        blob: usize,
        blobs: usize,
        pending: Pending,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Pending {
    pub start: Cursor,
    pub end: Cursor,
    pub input_rows: u64,
    pub eof: bool,
    pub digest: String,
    pub observed_at: DateTime<Utc>,
    pub batch_count: usize,
    pub completed: BTreeSet<usize>,
}

#[derive(Debug)]
pub enum Event {
    ReportRequested(String),
    ReportReady(usize),
    EmptyDay,
    SourceOpened(Cursor),
    ChunkPrepared(Pending),
    BatchAccepted { index: usize, records: u64 },
    ChunkCommitted,
    BlobFinished,
    DayFinished,
}

impl State {
    pub fn new(day: NaiveDate, fingerprint: String) -> Self {
        Self {
            version: 1,
            fingerprint,
            day,
            phase: Phase::Ready,
            totals: Totals::default(),
        }
    }

    pub fn transition(&self, event: Event) -> Result<Self> {
        let mut next = self.clone();
        next.phase = match (&self.phase, event) {
            (Phase::Ready, Event::ReportRequested(operation)) => {
                Phase::AwaitingReport { operation }
            }
            (Phase::AwaitingReport { operation }, Event::ReportReady(blobs)) => Phase::Reading {
                operation: operation.clone(),
                blob: 0,
                blobs,
                cursor: Cursor::default(),
            },
            (Phase::Ready | Phase::AwaitingReport { .. }, Event::EmptyDay)
            | (Phase::Reading { .. }, Event::DayFinished)
                if !matches!(&self.phase, Phase::Reading { blob, blobs, .. } if blob != blobs) =>
            {
                next.day = self
                    .day
                    .succ_opt()
                    .ok_or_else(|| anyhow::anyhow!("date overflow"))?;
                next.totals.completed_days += 1;
                Phase::Ready
            }
            (
                Phase::Reading {
                    operation,
                    blob,
                    blobs,
                    cursor,
                },
                Event::SourceOpened(opened),
            ) => {
                ensure!(
                    cursor.offset == 0 && cursor.row == 0 && blob < blobs,
                    "source is already open"
                );
                ensure!(
                    opened.offset > 0 && opened.row == 0 && !opened.headers.is_empty(),
                    "source header was not read"
                );
                Phase::Reading {
                    operation: operation.clone(),
                    blob: *blob,
                    blobs: *blobs,
                    cursor: opened,
                }
            }
            (
                Phase::Reading {
                    operation,
                    blob,
                    blobs,
                    cursor,
                },
                Event::ChunkPrepared(pending),
            ) => {
                ensure!(
                    &pending.start == cursor,
                    "prepared chunk does not start at the checkpoint"
                );
                ensure!(
                    blob < blobs
                        && pending.batch_count > 0
                        && pending.input_rows > 0
                        && pending.end.offset > cursor.offset,
                    "empty or nonadvancing chunk"
                );
                ensure!(
                    pending.completed.is_empty(),
                    "new chunk already contains acknowledgements"
                );
                Phase::Writing {
                    operation: operation.clone(),
                    blob: *blob,
                    blobs: *blobs,
                    pending,
                }
            }
            (
                Phase::Writing {
                    operation,
                    blob,
                    blobs,
                    pending,
                },
                Event::BatchAccepted { index, records },
            ) => {
                ensure!(
                    index < pending.batch_count,
                    "batch index is outside the pending chunk"
                );
                let mut pending = pending.clone();
                ensure!(
                    pending.completed.insert(index),
                    "batch was already acknowledged"
                );
                next.totals.output_rows += records;
                next.totals.accepted_batches += 1;
                Phase::Writing {
                    operation: operation.clone(),
                    blob: *blob,
                    blobs: *blobs,
                    pending,
                }
            }
            (
                Phase::Writing {
                    operation,
                    blob,
                    blobs,
                    pending,
                },
                Event::ChunkCommitted,
            ) => {
                ensure!(
                    pending.completed.len() == pending.batch_count
                        && pending
                            .completed
                            .iter()
                            .all(|index| *index < pending.batch_count),
                    "cannot commit an unacknowledged chunk"
                );
                next.totals.input_rows += pending.input_rows;
                Phase::Reading {
                    operation: operation.clone(),
                    blob: if pending.eof { blob + 1 } else { *blob },
                    blobs: *blobs,
                    cursor: if pending.eof {
                        Cursor::default()
                    } else {
                        pending.end.clone()
                    },
                }
            }
            (
                Phase::Reading {
                    operation,
                    blob,
                    blobs,
                    ..
                },
                Event::BlobFinished,
            ) if blob < blobs => Phase::Reading {
                operation: operation.clone(),
                blob: blob + 1,
                blobs: *blobs,
                cursor: Cursor::default(),
            },
            (phase, event) => bail!("invalid migration transition: {phase:?} -> {event:?}"),
        };
        Ok(next)
    }
}

use crate::{azure::Azure, config::Config};
use anyhow::Context;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const LEASE_SECONDS: i64 = 120;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Checkpoint {
    #[serde(rename = "PartitionKey")]
    partition: String,
    #[serde(rename = "RowKey")]
    job: String,
    #[serde(rename = "Owner")]
    owner: String,
    #[serde(rename = "LeaseUntil")]
    lease_until: DateTime<Utc>,
    #[serde(rename = "State")]
    state: String,
}

struct Owned {
    checkpoint: Checkpoint,
    etag: String,
}

pub struct Lease {
    azure: Azure,
    config: Config,
    inner: Arc<Mutex<Owned>>,
    lost: Arc<AtomicBool>,
    stop: CancellationToken,
    heartbeat: Option<JoinHandle<Result<()>>>,
}

impl Lease {
    pub async fn acquire(azure: &Azure, config: &Config) -> Result<Self> {
        let owner = Uuid::new_v4().to_string();
        let deadline = Instant::now() + Duration::from_secs(180);
        let mut races = 0;
        let owned = loop {
            let response = azure
                .table_request(config, Method::GET, true, None, None)
                .await?;
            if response.status() == StatusCode::FORBIDDEN
                && config.azure.provision
                && Instant::now() < deadline
            {
                tracing::info!("waiting for Storage Table RBAC propagation");
                azure.sleep(Duration::from_secs(5)).await?;
                continue;
            }
            let (mut checkpoint, etag) = if response.status() == StatusCode::NOT_FOUND {
                (
                    Checkpoint {
                        partition: "jobs".into(),
                        job: config.job.id.clone(),
                        owner: String::new(),
                        lease_until: Utc::now(),
                        state: encode_state(&State::new(
                            config.job.start_date,
                            config.fingerprint()?,
                        ))?,
                    },
                    None,
                )
            } else {
                let response = Azure::expect(response, &[StatusCode::OK]).await?;
                let etag = response_etag(&response)?;
                let checkpoint: Checkpoint = serde_json::from_value(Azure::json(response).await?)?;
                (checkpoint, Some(etag))
            };
            let state: State = serde_json::from_str(&checkpoint.state)
                .context("decoding persisted migration state")?;
            ensure!(
                state.version == 1,
                "unsupported checkpoint version {}",
                state.version
            );
            ensure!(
                state.fingerprint == config.fingerprint()?,
                "job configuration differs from its checkpoint; restore the original source, aggregation, chunk limits and destination, or choose a new job.id"
            );
            ensure!(
                checkpoint.owner.is_empty() || checkpoint.lease_until <= Utc::now(),
                "job {} is already leased until {}; another run may be active",
                config.job.id,
                checkpoint.lease_until
            );
            checkpoint.owner = owner.clone();
            checkpoint.lease_until = Utc::now() + chrono::Duration::seconds(LEASE_SECONDS);
            let response = azure
                .table_request(
                    config,
                    if etag.is_some() {
                        Method::PUT
                    } else {
                        Method::POST
                    },
                    etag.is_some(),
                    etag.as_deref(),
                    Some(serde_json::to_value(&checkpoint)?),
                )
                .await?;
            if matches!(
                response.status(),
                StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED
            ) {
                races += 1;
                ensure!(
                    races < 8,
                    "could not acquire job lease due to concurrent checkpoint updates"
                );
                continue;
            }
            let response = Azure::expect(
                response,
                &[StatusCode::OK, StatusCode::CREATED, StatusCode::NO_CONTENT],
            )
            .await?;
            break Owned {
                checkpoint,
                etag: response_etag(&response)?,
            };
        };
        let control = azure.with_cancellation(CancellationToken::new());
        let inner = Arc::new(Mutex::new(owned));
        let lost = Arc::new(AtomicBool::new(false));
        let stop = CancellationToken::new();
        let heartbeat = {
            let (control, config, inner, lost, stop, cancellation) = (
                control.clone(),
                config.clone(),
                inner.clone(),
                lost.clone(),
                stop.clone(),
                azure.cancellation.clone(),
            );
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = stop.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                    }
                    let update = async {
                        let mut owned = inner.lock().await;
                        check_owned(&owned, &lost)?;
                        replace(&control, &config, &mut owned, None, false).await
                    };
                    match tokio::time::timeout(Duration::from_secs(30), update).await {
                        Ok(Ok(())) => {}
                        result => {
                            lost.store(true, Ordering::SeqCst);
                            cancellation.cancel();
                            return match result {
                                Ok(Err(error)) => Err(error)
                                    .context("job lease renewal failed; migration was cancelled"),
                                Err(error) => Err(error).context(
                                    "job lease renewal timed out; migration was cancelled",
                                ),
                                Ok(Ok(())) => unreachable!(),
                            };
                        }
                    }
                }
            })
        };
        Ok(Self {
            azure: control,
            config: config.clone(),
            inner,
            lost,
            stop,
            heartbeat: Some(heartbeat),
        })
    }

    pub async fn state(&self) -> Result<State> {
        let owned = self.inner.lock().await;
        check_owned(&owned, &self.lost)?;
        serde_json::from_str(&owned.checkpoint.state).context("decoding owned checkpoint")
    }

    pub async fn apply(&self, event: Event) -> Result<State> {
        let mut owned = self.inner.lock().await;
        check_owned(&owned, &self.lost)?;
        let state: State = serde_json::from_str(&owned.checkpoint.state)?;
        let next = state.transition(event)?;
        if let Err(error) = replace(&self.azure, &self.config, &mut owned, Some(&next), false).await
        {
            self.lost.store(true, Ordering::SeqCst);
            return Err(error)
                .context("checkpoint could not be committed; stopping to preserve lease fencing");
        }
        Ok(next)
    }

    pub async fn ensure_owned(&self) -> Result<()> {
        let owned = self.inner.lock().await;
        check_owned(&owned, &self.lost)
    }

    pub async fn finish(mut self) -> Result<()> {
        self.stop.cancel();
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.await.context("lease heartbeat task failed")??;
        }
        let mut owned = self.inner.lock().await;
        check_owned(&owned, &self.lost)?;
        replace(&self.azure, &self.config, &mut owned, None, true)
            .await
            .context("releasing job lease")
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

fn check_owned(owned: &Owned, lost: &AtomicBool) -> Result<()> {
    ensure!(!lost.load(Ordering::SeqCst), "job lease was lost");
    ensure!(
        !owned.checkpoint.owner.is_empty()
            && owned.checkpoint.lease_until > Utc::now() + chrono::Duration::seconds(10),
        "job lease is expired or too close to expiry; refusing unfenced work"
    );
    Ok(())
}

async fn replace(
    azure: &Azure,
    config: &Config,
    owned: &mut Owned,
    state: Option<&State>,
    release: bool,
) -> Result<()> {
    let mut checkpoint = owned.checkpoint.clone();
    checkpoint.lease_until = Utc::now() + chrono::Duration::seconds(LEASE_SECONDS);
    if let Some(state) = state {
        checkpoint.state = encode_state(state)?;
    }
    if release {
        checkpoint.owner.clear();
        checkpoint.lease_until = Utc::now();
    }
    let response = azure
        .table_request(
            config,
            Method::PUT,
            true,
            Some(&owned.etag),
            Some(serde_json::to_value(&checkpoint)?),
        )
        .await?;
    ensure!(
        response.status() != StatusCode::PRECONDITION_FAILED,
        "checkpoint ETag changed; another owner fenced this run"
    );
    let response = Azure::expect(response, &[StatusCode::OK, StatusCode::NO_CONTENT]).await?;
    owned.etag = response_etag(&response)?;
    owned.checkpoint = checkpoint;
    Ok(())
}

fn encode_state(state: &State) -> Result<String> {
    let encoded = serde_json::to_string(state)?;
    ensure!(
        encoded.encode_utf16().count() <= 30_000,
        "checkpoint exceeds the Azure Table string-property limit; reduce CSV header size or chunk_bytes"
    );
    Ok(encoded)
}

fn response_etag(response: &reqwest::Response) -> Result<String> {
    Ok(response
        .headers()
        .get(reqwest::header::ETAG)
        .context("Azure Table response has no ETag; cannot safely lock or checkpoint")?
        .to_str()?
        .to_owned())
}

pub async fn status(azure: &Azure, config: &Config) -> Result<Value> {
    let response = azure
        .table_request(config, Method::GET, true, None, None)
        .await?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(json!({"job": config.job.id, "checkpoint": null}));
    }
    let checkpoint: Checkpoint = serde_json::from_value(
        Azure::json(Azure::expect(response, &[StatusCode::OK]).await?).await?,
    )?;
    let state: State = serde_json::from_str(&checkpoint.state)?;
    Ok(
        json!({"job": config.job.id, "owner": checkpoint.owner, "lease_until": checkpoint.lease_until, "checkpoint": state}),
    )
}
#[cfg(test)]
mod tests {
    use super::*;

    fn initial() -> State {
        State::new(
            NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            "fingerprint".into(),
        )
    }

    #[test]
    fn state_machine_requires_all_acks_and_round_trips_mid_write() {
        let ready = initial();
        let requested = ready
            .transition(Event::ReportRequested("operation".into()))
            .unwrap();
        let reading = requested.transition(Event::ReportReady(1)).unwrap();
        let pending = Pending {
            start: Cursor::default(),
            end: Cursor {
                offset: 100,
                row: 10,
                ..Cursor::default()
            },
            input_rows: 10,
            eof: true,
            digest: "digest".into(),
            observed_at: Utc::now(),
            batch_count: 2,
            completed: BTreeSet::new(),
        };
        let writing = reading.transition(Event::ChunkPrepared(pending)).unwrap();
        assert!(writing.transition(Event::ChunkCommitted).is_err());
        let one = writing
            .transition(Event::BatchAccepted {
                index: 1,
                records: 2,
            })
            .unwrap();
        assert!(
            one.transition(Event::BatchAccepted {
                index: 1,
                records: 2
            })
            .is_err()
        );
        assert!(
            one.transition(Event::BatchAccepted {
                index: 2,
                records: 2
            })
            .is_err()
        );
        let saved: State = serde_json::from_str(&serde_json::to_string(&one).unwrap()).unwrap();
        assert_eq!(saved, one);
        let all = saved
            .transition(Event::BatchAccepted {
                index: 0,
                records: 3,
            })
            .unwrap();
        let committed = all.transition(Event::ChunkCommitted).unwrap();
        assert_eq!(committed.totals.input_rows, 10);
        assert_eq!(committed.totals.output_rows, 5);
        assert!(matches!(committed.phase, Phase::Reading { blob: 1, .. }));
        let finished = committed.transition(Event::DayFinished).unwrap();
        assert_eq!(finished.day.to_string(), "2026-01-02");
        assert_eq!(finished.totals.completed_days, 1);
    }

    #[test]
    fn invalid_transitions_never_change_state() {
        let state = initial();
        assert!(state.transition(Event::ChunkCommitted).is_err());
        assert!(state.transition(Event::ReportReady(1)).is_err());
        assert!(state.transition(Event::DayFinished).is_err());
        assert!(
            state
                .transition(Event::BatchAccepted {
                    index: 0,
                    records: 1
                })
                .is_err()
        );
        assert_eq!(state, initial());
    }
}
