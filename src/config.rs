use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, path::Path};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub job: Job,
    pub azure: Resources,
    pub target: Target,
    #[serde(default)]
    pub aggregation: Aggregation,
    #[serde(default)]
    pub limits: Limits,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub id: String,
    pub scope: String,
    pub start_date: NaiveDate,
    #[serde(default)]
    pub metric: Metric,
    #[serde(default = "default_lag")]
    pub lag_days: u32,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub enum Metric {
    #[default]
    ActualCost,
    AmortizedCost,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub subscription_id: Uuid,
    pub resource_group: String,
    pub location: String,
    pub storage_account: String,
    pub workspace: String,
    pub dcr: String,
    #[serde(default = "default_table")]
    pub table: String,
    #[serde(default = "yes")]
    pub provision: bool,
    pub principal_id: Option<Uuid>,
    pub workspace_capacity_gb: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Target {
    Direct,
    EventHub {
        namespace: String,
        hub: String,
        #[serde(default = "default_consumer_group")]
        consumer_group: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Aggregation {
    pub dimensions: Vec<String>,
    pub cost_column: Option<String>,
    pub date_column: Option<String>,
    pub currency_column: Option<String>,
}

impl Default for Aggregation {
    fn default() -> Self {
        Self {
            dimensions: vec!["SubscriptionId".into(), "ConsumedService".into()],
            cost_column: None,
            date_column: None,
            currency_column: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub chunk_rows: usize,
    pub chunk_bytes: usize,
    pub batch_bytes: usize,
    pub concurrency: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            chunk_rows: 5_000,
            chunk_bytes: 8_000_000,
            batch_bytes: 900_000,
            concurrency: 4,
        }
    }
}

fn default_lag() -> u32 {
    3
}

fn default_table() -> String {
    "AzureCosts_CL".into()
}

fn default_consumer_group() -> String {
    "gotomoch".into()
}

fn yes() -> bool {
    true
}

impl Config {
    pub async fn load(path: &Path) -> Result<Self> {
        let text = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("reading configuration {}", path.display()))?;
        let config: Self = toml::from_str(&text).context("parsing configuration")?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.job.id.is_empty()
                && self.job.id.len() <= 64
                && self
                    .job
                    .id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
            "job.id must contain 1-64 ASCII letters, digits, hyphens, or underscores"
        );
        ensure!(
            (self.job.scope.starts_with("/subscriptions/")
                || self.job.scope.starts_with("/providers/Microsoft.Billing/"))
                && !self.job.scope.ends_with('/')
                && !self.job.scope.contains("..")
                && self
                    .job
                    .scope
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"/-_.:".contains(&c)),
            "job.scope must be an Azure subscription or Microsoft.Billing resource ID"
        );
        ensure!(self.job.lag_days >= 1, "job.lag_days must be at least 1");
        ensure!(
            (3..=24).contains(&self.azure.storage_account.len())
                && self
                    .azure
                    .storage_account
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "azure.storage_account must contain 3-24 lowercase letters or digits"
        );
        for (label, name) in [
            ("resource_group", &self.azure.resource_group),
            ("location", &self.azure.location),
            ("workspace", &self.azure.workspace),
            ("dcr", &self.azure.dcr),
            ("table", &self.azure.table),
        ] {
            validate_name(label, name)?;
        }
        ensure!(
            (4..=63).contains(&self.azure.workspace.len())
                && self
                    .azure
                    .workspace
                    .starts_with(|c: char| c.is_ascii_alphanumeric())
                && self
                    .azure
                    .workspace
                    .ends_with(|c: char| c.is_ascii_alphanumeric())
                && self
                    .azure
                    .workspace
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-'),
            "azure.workspace must contain 4-63 letters, digits, or hyphens and start and end with a letter or digit"
        );
        ensure!(
            self.azure.dcr.len() <= 55,
            "azure.dcr must have at most 55 characters to leave room for the DCE suffix"
        );
        ensure!(
            self.azure.table.ends_with("_CL")
                && self.azure.table.len() <= 45
                && self
                    .azure
                    .table
                    .starts_with(|c: char| c.is_ascii_alphabetic()),
            "azure.table must start with a letter, end in _CL, and have at most 45 characters"
        );
        ensure!(
            self.azure
                .table
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_'),
            "azure.table may contain only letters, digits, and underscores"
        );
        if let Some(capacity) = self.azure.workspace_capacity_gb {
            ensure!(
                capacity >= 100,
                "workspace_capacity_gb must be at least 100; this is a paid daily commitment"
            );
        }
        if let Target::EventHub {
            namespace,
            hub,
            consumer_group,
        } = &self.target
        {
            validate_name("Event Hub namespace", namespace)?;
            validate_name("Event Hub", hub)?;
            if consumer_group != "$Default" {
                validate_name("consumer group", consumer_group)?;
            }
            ensure!(
                consumer_group.len() <= 50,
                "Event Hub consumer group must have at most 50 characters"
            );
            ensure!(
                (6..=50).contains(&namespace.len())
                    && namespace.starts_with(|c: char| c.is_ascii_alphabetic())
                    && namespace.ends_with(|c: char| c.is_ascii_alphanumeric())
                    && namespace
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'-'),
                "Event Hub namespace must contain 6-50 letters, digits, or hyphens and start with a letter"
            );
        }
        ensure!(
            self.aggregation.dimensions.len() <= 16,
            "at most 16 dimensions are supported"
        );
        let mut dimensions = BTreeSet::new();
        for dimension in &self.aggregation.dimensions {
            ensure!(
                !normalize(dimension).is_empty() && dimension.len() <= 128,
                "aggregation dimensions need an alphanumeric name of at most 128 bytes"
            );
            ensure!(
                dimensions.insert(normalize(dimension)),
                "duplicate aggregation dimension: {dimension}"
            );
        }
        for name in [
            &self.aggregation.cost_column,
            &self.aggregation.date_column,
            &self.aggregation.currency_column,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(
                !normalize(name).is_empty() && name.len() <= 128,
                "column overrides need an alphanumeric name of at most 128 bytes"
            );
        }
        ensure!(
            (1..=100_000).contains(&self.limits.chunk_rows),
            "chunk_rows must be between 1 and 100000"
        );
        ensure!(
            (1_000_000..=64_000_000).contains(&self.limits.chunk_bytes),
            "chunk_bytes must be between 1000000 and 64000000"
        );
        ensure!(
            (64_000..=950_000).contains(&self.limits.batch_bytes),
            "batch_bytes must be between 64000 and 950000"
        );
        ensure!(
            (1..=16).contains(&self.limits.concurrency),
            "concurrency must be between 1 and 16"
        );
        Ok(())
    }

    pub fn fingerprint(&self) -> Result<String> {
        // Operational controls may change; source, grouping, chunk boundaries and destination may not.
        let data = serde_json::json!({
            "version": 1,
            "id": self.job.id,
            "scope": self.job.scope,
            "start_date": self.job.start_date,
            "metric": self.job.metric,
            "aggregation": self.aggregation,
            "chunk_rows": self.limits.chunk_rows,
            "chunk_bytes": self.limits.chunk_bytes,
            "batch_bytes": self.limits.batch_bytes,
            "target": self.target,
            "storage_account": self.azure.storage_account,
            "subscription": self.azure.subscription_id,
            "resource_group": self.azure.resource_group,
            "workspace": self.azure.workspace,
            "table": self.azure.table,
            "dcr": self.azure.dcr,
        });
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&data)?)))
    }

    pub fn group_id(&self) -> String {
        format!(
            "/subscriptions/{}/resourceGroups/{}",
            self.azure.subscription_id, self.azure.resource_group
        )
    }

    pub fn resource_id(&self, provider: &str, kind: &str, name: &str) -> String {
        format!("{}/providers/{provider}/{kind}/{name}", self.group_id())
    }
}

fn validate_name(label: &str, name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 90
            && name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
            && !name.ends_with('.'),
        "{label} must contain 1-90 ASCII letters, digits, hyphens, underscores, or dots and not end in a dot"
    );
    Ok(())
}

pub fn normalize(name: &str) -> String {
    name.chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect()
}
