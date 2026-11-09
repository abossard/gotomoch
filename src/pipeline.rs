use crate::config::{Config, Metric, Target, normalize};
use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use csv_async::{AsyncReader, AsyncReaderBuilder, StringRecord};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, pin::Pin, str::FromStr};
use tokio::io::AsyncRead;

pub type Input = Pin<Box<dyn AsyncRead + Send>>;
pub const MAX_RECORD_BYTES: usize = 1_000_000;

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Cursor {
    pub offset: u64,
    pub row: u64,
    pub headers: Vec<String>,
    pub etag: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct GroupKey {
    date: NaiveDate,
    currency: String,
    dimensions: Vec<String>,
}

#[derive(Debug)]
struct Total {
    cost: Decimal,
    rows: u64,
}

pub struct Chunk {
    pub start: Cursor,
    pub end: Cursor,
    pub input_rows: u64,
    pub eof: bool,
    pub digest: String,
    groups: BTreeMap<GroupKey, Total>,
}

#[derive(Clone, Debug)]
pub struct Batch {
    pub body: Bytes,
    pub records: u64,
}

pub struct CsvSource {
    reader: AsyncReader<Input>,
    base: u64,
    cursor: Cursor,
    columns: Columns,
}

struct Columns {
    date: usize,
    currency: usize,
    cost: usize,
    dimensions: Vec<usize>,
    count: usize,
}

impl CsvSource {
    pub async fn new(input: Input, mut cursor: Cursor, config: &Config) -> Result<Self> {
        let base = cursor.offset;
        let mut reader = AsyncReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .create_reader(input);
        if base == 0 {
            let mut record = StringRecord::new();
            ensure!(
                reader.read_record(&mut record).await?,
                "CSV is empty: expected a header"
            );
            let headers: Vec<String> = record
                .iter()
                .map(|s| s.trim_start_matches('\u{feff}').to_owned())
                .collect();
            ensure!(
                cursor.headers.is_empty() || cursor.headers == headers,
                "CSV header changed while resuming"
            );
            cursor.headers = headers;
            cursor.offset = reader.position().byte();
        }
        ensure!(
            !cursor.headers.is_empty(),
            "resumed CSV has no saved header"
        );
        let columns = Columns::new(&cursor.headers, config)?;
        Ok(Self {
            reader,
            base,
            cursor,
            columns,
        })
    }

    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    pub async fn read_chunk(
        &mut self,
        config: &Config,
        expected_day: Option<NaiveDate>,
    ) -> Result<Chunk> {
        let start = self.cursor.clone();
        let mut groups: BTreeMap<GroupKey, Total> = BTreeMap::new();
        let mut digest = Sha256::new();
        let mut input_rows = 0_u64;
        let mut input_bytes = 0_usize;
        let mut eof = false;
        let mut record = StringRecord::new();
        while input_rows < config.limits.chunk_rows as u64
            && input_bytes < config.limits.chunk_bytes
        {
            if !self
                .reader
                .read_record(&mut record)
                .await
                .with_context(|| format!("reading CSV record {}", self.cursor.row + 1))?
            {
                eof = true;
                break;
            }
            ensure!(
                record.len() == self.columns.count,
                "CSV record {} has {} fields; expected {}",
                self.cursor.row + 1,
                record.len(),
                self.columns.count
            );
            let bytes: usize = record.iter().map(str::len).sum();
            ensure!(
                bytes <= MAX_RECORD_BYTES,
                "CSV record {} exceeds 1 MB",
                self.cursor.row + 1
            );
            digest.update(serde_json::to_vec(&record.iter().collect::<Vec<_>>())?);
            let (key, cost) = self
                .columns
                .parse(&record, expected_day)
                .with_context(|| format!("validating CSV record {}", self.cursor.row + 1))?;
            let total = groups.entry(key).or_insert(Total {
                cost: Decimal::ZERO,
                rows: 0,
            });
            total.cost = total
                .cost
                .checked_add(cost)
                .context("decimal cost total overflow")?;
            total.rows = total.rows.checked_add(1).context("row count overflow")?;
            input_rows += 1;
            input_bytes += bytes;
            self.cursor.row += 1;
        }
        self.cursor.offset = self
            .base
            .checked_add(self.reader.position().byte())
            .context("CSV offset overflow")?;
        Ok(Chunk {
            start,
            end: self.cursor.clone(),
            input_rows,
            eof,
            digest: format!("{:x}", digest.finalize()),
            groups,
        })
    }
}

impl Columns {
    fn new(headers: &[String], config: &Config) -> Result<Self> {
        let mut names = BTreeMap::new();
        for (index, header) in headers.iter().enumerate() {
            let normalized = normalize(header);
            ensure!(!normalized.is_empty(), "CSV has a blank column name");
            ensure!(
                names.insert(normalized, index).is_none(),
                "ambiguous or duplicate CSV column: {header}"
            );
        }
        let find = |explicit: Option<&str>, aliases: &[&str]| -> Result<usize> {
            if let Some(name) = explicit {
                return names
                    .get(&normalize(name))
                    .copied()
                    .with_context(|| format!("CSV is missing configured column {name}"));
            }
            aliases
                .iter()
                .find_map(|name| names.get(*name).copied())
                .with_context(|| {
                    format!(
                        "CSV is missing a required column; expected one of {}",
                        aliases.join(", ")
                    )
                })
        };
        let focus_cost = match config.job.metric {
            Metric::ActualCost => "billedcost",
            Metric::AmortizedCost => "effectivecost",
        };
        let dimensions = config
            .aggregation
            .dimensions
            .iter()
            .map(|name| {
                let aliases = match normalize(name).as_str() {
                    "subscriptionid" => &["subscriptionid", "subscriptionguid", "subaccountid"][..],
                    "consumedservice" => &["consumedservice", "servicename"][..],
                    _ => &[][..],
                };
                if aliases.is_empty() {
                    find(Some(name), &[])
                } else {
                    find(None, aliases)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            date: find(
                config.aggregation.date_column.as_deref(),
                &["date", "usagedate", "usagedatetime", "chargeperiodstart"],
            )?,
            currency: find(
                config.aggregation.currency_column.as_deref(),
                &["billingcurrencycode", "billingcurrency", "currency"],
            )?,
            cost: find(
                config.aggregation.cost_column.as_deref(),
                &["costinbillingcurrency", "pretaxcost", "cost", focus_cost],
            )?,
            dimensions,
            count: headers.len(),
        })
    }

    fn parse(
        &self,
        record: &StringRecord,
        expected_day: Option<NaiveDate>,
    ) -> Result<(GroupKey, Decimal)> {
        let date = parse_date(record[self.date].trim())?;
        if let Some(expected) = expected_day {
            ensure!(
                date == expected,
                "report contains date {date}, but the requested day is {expected}"
            );
        }
        let currency = record[self.currency].trim().to_uppercase();
        ensure!(
            currency.len() == 3 && currency.bytes().all(|c| c.is_ascii_uppercase()),
            "billing currency must be a three-letter currency code"
        );
        let value = record[self.cost].trim();
        let cost = if let Some((base, _)) = value.split_once(['e', 'E']) {
            Decimal::from_str_exact(base).context("cost mantissa exceeds decimal precision")?;
            Decimal::from_scientific(value)
        } else {
            Decimal::from_str_exact(value)
        }
        .context("cost must be a finite decimal number within supported precision")?;
        Ok((
            GroupKey {
                date,
                currency,
                dimensions: self
                    .dimensions
                    .iter()
                    .map(|&index| record[index].to_owned())
                    .collect(),
            },
            cost,
        ))
    }
}

fn parse_date(value: &str) -> Result<NaiveDate> {
    for format in ["%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"] {
        if let Ok(date) = NaiveDate::parse_from_str(value, format) {
            return Ok(date);
        }
    }
    if let Ok(date) = DateTime::parse_from_rfc3339(value) {
        return Ok(date.date_naive());
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%m/%d/%Y %H:%M:%S",
    ] {
        if let Ok(date) = NaiveDateTime::parse_from_str(value, format) {
            return Ok(date.date());
        }
    }
    bail!("unsupported cost date; expected ISO 8601, YYYYMMDD, or MM/DD/YYYY")
}

impl Chunk {
    pub fn records(
        &self,
        config: &Config,
        blob: usize,
        observed_at: DateTime<Utc>,
    ) -> Result<Vec<Value>> {
        self.groups
            .iter()
            .map(|(key, total)| {
                let identity = serde_json::to_vec(&(
                    &config.job.id,
                    blob,
                    self.start.offset,
                    &self.digest,
                    key,
                ))?;
                let dimensions: BTreeMap<&str, &str> = config
                    .aggregation
                    .dimensions
                    .iter()
                    .zip(&key.dimensions)
                    .map(|(name, value)| (name.as_str(), value.as_str()))
                    .collect();
                let cost = Number::from_str(&total.cost.normalize().to_string())?;
                Ok(json!({
                    "TimeGenerated": observed_at.to_rfc3339(),
                    "CostDate": format!("{}T00:00:00Z", key.date),
                    "Currency": key.currency,
                    "Metric": config.job.metric,
                    "Dimensions": dimensions,
                    "Cost": cost,
                    "Rows": total.rows,
                    "RecordId": format!("{:x}", Sha256::digest(identity)),
                    "JobId": config.job.id,
                }))
            })
            .collect()
    }

    pub fn batches(
        &self,
        config: &Config,
        blob: usize,
        observed_at: DateTime<Utc>,
    ) -> Result<Vec<Batch>> {
        encode_batches(
            self.records(config, blob, observed_at)?,
            &config.target,
            config.limits.batch_bytes,
        )
    }
}

pub fn encode_batches(records: Vec<Value>, target: &Target, limit: usize) -> Result<Vec<Batch>> {
    let mut batches = Vec::new();
    let mut body = vec![b'['];
    let mut count = 0_u64;
    for record in records {
        let raw = serde_json::to_vec(&record)?;
        ensure!(
            raw.len() <= 60_000,
            "aggregated record exceeds 60000 bytes; select fewer or smaller dimensions"
        );
        let encoded = match target {
            Target::Direct => raw,
            Target::EventHub { .. } => serde_json::to_vec(&json!({
                "Body": String::from_utf8(raw)?,
            }))?,
        };
        ensure!(
            encoded.len() + 2 <= limit,
            "one encoded record exceeds the configured batch_bytes"
        );
        if body.len() + usize::from(count > 0) + encoded.len() + 1 > limit {
            body.push(b']');
            batches.push(Batch {
                body: Bytes::from(body),
                records: count,
            });
            body = vec![b'['];
            count = 0;
        }
        if count > 0 {
            body.push(b',');
        }
        body.extend_from_slice(&encoded);
        count += 1;
    }
    if count > 0 {
        body.push(b']');
        batches.push(Batch {
            body: Bytes::from(body),
            records: count,
        });
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        toml::from_str(include_str!("../examples/direct.toml")).unwrap()
    }

    #[tokio::test]
    async fn aggregates_exact_decimals_and_preserves_currency_and_date() {
        let config = config();
        let csv = "Date,CostInBillingCurrency,BillingCurrencyCode,SubscriptionId,ConsumedService\n\
            2026-01-01,0.1,USD,s1,Compute\n2026-01-01,0.2,USD,s1,Compute\n\
            2026-01-01,-0.05,USD,s1,Compute\n2026-01-01,2,EUR,s1,Compute\n\
            2026-01-02,3,USD,s1,Compute\n";
        let mut source = CsvSource::new(Box::pin(csv.as_bytes()), Cursor::default(), &config)
            .await
            .unwrap();
        let chunk = source.read_chunk(&config, None).await.unwrap();
        let records = chunk.records(&config, 0, Utc::now()).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[1]["Cost"].to_string(), "0.25");
        assert_eq!(records[1]["Rows"], 3);
        assert_eq!(records[1]["Dimensions"]["ConsumedService"], "Compute");
        assert!(chunk.eof);
    }

    #[tokio::test]
    async fn resumes_at_exact_byte_boundary_with_quoted_newlines() {
        let mut config = config();
        config.limits.chunk_rows = 1;
        let csv = "Date,CostInBillingCurrency,BillingCurrencyCode,SubscriptionId,ConsumedService\r\n\
            2026-01-01,1,USD,s1,\"Compute\nand more\"\r\n2026-01-01,2,USD,s1,Storage\r\n";
        let mut source = CsvSource::new(Box::pin(csv.as_bytes()), Cursor::default(), &config)
            .await
            .unwrap();
        let first = source.read_chunk(&config, None).await.unwrap();
        let mut resumed = CsvSource::new(
            Box::pin(&csv.as_bytes()[first.end.offset as usize..]),
            first.end.clone(),
            &config,
        )
        .await
        .unwrap();
        let next = resumed.read_chunk(&config, None).await.unwrap();
        assert_eq!(next.start.row, 1);
        assert_eq!(next.end.row, 2);
        assert_eq!(next.records(&config, 0, Utc::now()).unwrap()[0]["Cost"], 2);
        let end = resumed.read_chunk(&config, None).await.unwrap();
        assert_eq!(end.input_rows, 0);
        assert!(end.eof);
        assert_eq!(end.end.offset as usize, csv.len());
    }

    #[tokio::test]
    async fn rejects_missing_dimensions_invalid_costs_and_wrong_dates() {
        let config = config();
        let missing = "Date,Cost,Currency\n2026-01-01,1,USD\n";
        assert!(
            CsvSource::new(Box::pin(missing.as_bytes()), Cursor::default(), &config)
                .await
                .is_err()
        );
        for row in [
            "2026-01-01,NaN,USD,s1,Compute",
            "2026-01-02,1,USD,s1,Compute",
        ] {
            let text = format!("Date,Cost,Currency,SubscriptionId,ConsumedService\n{row}\n");
            let mut source = CsvSource::new(
                Box::pin(std::io::Cursor::new(text.into_bytes())),
                Cursor::default(),
                &config,
            )
            .await
            .unwrap();
            assert!(
                source
                    .read_chunk(&config, Some(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()))
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn focus_uses_effective_cost_for_amortization() {
        let mut config = config();
        config.job.metric = Metric::AmortizedCost;
        let csv = "ChargePeriodStart,BilledCost,EffectiveCost,BillingCurrency,SubAccountId,ServiceName\n\
            2026-01-01T00:00:00Z,10,2.50,USD,s1,Compute\n";
        let mut source = CsvSource::new(Box::pin(csv.as_bytes()), Cursor::default(), &config)
            .await
            .unwrap();
        let chunk = source.read_chunk(&config, None).await.unwrap();
        assert_eq!(
            chunk.records(&config, 0, Utc::now()).unwrap()[0]["Cost"].to_string(),
            "2.5"
        );
    }

    #[tokio::test]
    async fn accepts_native_timestamps_and_scientific_costs_without_rounding_input() {
        let config = config();
        for (cost, accepted) in [
            ("1e-3", true),
            ("0.00000000000000000000000000001", false),
            ("NaN", false),
        ] {
            let csv = format!(
                "UsageDateTime,PreTaxCost,Currency,SubscriptionGuid,ConsumedService\n2026-01-01T00:00:00,{cost},USD,s1,Compute\n"
            );
            let mut source = CsvSource::new(
                Box::pin(std::io::Cursor::new(csv.into_bytes())),
                Cursor::default(),
                &config,
            )
            .await
            .unwrap();
            let chunk = source.read_chunk(&config, None).await;
            assert_eq!(chunk.is_ok(), accepted);
            if let Ok(chunk) = chunk {
                assert_eq!(
                    chunk.records(&config, 0, Utc::now()).unwrap()[0]["Cost"].to_string(),
                    "0.001"
                );
            }
        }
    }

    #[test]
    fn measures_actual_encoded_bytes_for_both_transports() {
        for target in [
            Target::Direct,
            Target::EventHub {
                namespace: "example".into(),
                hub: "cost".into(),
                consumer_group: "cost".into(),
            },
        ] {
            let rows = (0..100)
                .map(|_| json!({"text": "\"\\\n".repeat(100)}))
                .collect();
            let batches = encode_batches(rows, &target, 4_000).unwrap();
            assert!(batches.len() > 1);
            assert_eq!(batches.iter().map(|b| b.records).sum::<u64>(), 100);
            for batch in batches {
                assert!(batch.body.len() <= 4_000);
                let parsed: Vec<Value> = serde_json::from_slice(&batch.body).unwrap();
                if matches!(target, Target::EventHub { .. }) {
                    let body = parsed[0]["Body"].as_str().unwrap();
                    assert!(serde_json::from_str::<Value>(body).is_ok());
                }
            }
        }
    }
}
