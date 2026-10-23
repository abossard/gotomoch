use crate::{
    config::{Config, Target},
    pipeline::{Batch, CsvSource, Cursor, Input},
};
use anyhow::{Context, Result, bail, ensure};
use async_compression::tokio::bufread::GzipDecoder;
use azure_core::auth::TokenCredential;
use azure_identity::DefaultAzureCredentialBuilder;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use futures_util::{StreamExt, TryStreamExt};
use reqwest::{
    Client, Method, Response, StatusCode, Url,
    header::{self, HeaderMap, HeaderValue},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::{io::StreamReader, sync::CancellationToken};
use uuid::Uuid;

pub const ARM_SCOPE: &str = "https://management.azure.com/.default";
pub const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";
pub const MONITOR_SCOPE: &str = "https://monitor.azure.com/.default";
pub const EVENT_HUB_SCOPE: &str = "https://eventhubs.azure.net/.default";
pub const STATE_TABLE: &str = "GotomochState";
const COST_API: &str = "2024-08-01";
const STORAGE_API: &str = "2023-05-01";
const MONITOR_API: &str = "2023-03-11";
const WORKSPACE_API: &str = "2023-09-01";
const EVENT_HUB_API: &str = "2024-01-01";
const ROLE_API: &str = "2022-04-01";
const TABLE_CONTRIBUTOR: &str = "0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3";
const METRICS_PUBLISHER: &str = "3913510d-42f4-4e42-8a64-420c390055eb";
const EVENT_SENDER: &str = "2b629674-e913-4c01-ae53-ef4638d8f975";
const EVENT_RECEIVER: &str = "a638d3c7-ab3a-418d-83e6-5f17a39d4fde";

#[derive(Clone)]
pub struct Azure {
    client: Client,
    credential: Arc<dyn TokenCredential>,
    pub cancellation: CancellationToken,
    providers: Arc<tokio::sync::Mutex<BTreeSet<String>>>,
    #[cfg(test)]
    test_origin: Option<String>,
}

#[derive(Clone)]
pub struct Sink {
    url: String,
    audience: &'static str,
    content_type: &'static str,
    success: StatusCode,
    propagation_deadline: Option<Instant>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Blob {
    pub blob_link: String,
    pub byte_count: u64,
}

impl Blob {
    pub fn is_gzip(&self, compressed: bool) -> Result<bool> {
        Ok(compressed || Url::parse(&self.blob_link)?.path().ends_with(".gz"))
    }
}

#[derive(Clone, Debug)]
pub struct Report {
    pub blobs: Vec<Blob>,
    pub compressed: bool,
    pub valid_till: Option<DateTime<Utc>>,
}

pub enum ReportStart {
    Pending { operation: String, delay: Duration },
    Ready { operation: String, report: Report },
    Empty,
}

pub enum ReportPoll {
    Pending(Duration),
    Ready(Report),
    Empty,
}

impl Azure {
    pub fn new(cancellation: CancellationToken) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .user_agent(concat!("gotomoch/", env!("CARGO_PKG_VERSION")))
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(15))
                .read_timeout(Duration::from_secs(60))
                .no_gzip()
                .build()?,
            credential: Arc::new(DefaultAzureCredentialBuilder::new().build()?),
            cancellation,
            providers: Arc::new(tokio::sync::Mutex::new(BTreeSet::new())),
            #[cfg(test)]
            test_origin: None,
        })
    }

    pub fn with_cancellation(&self, cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            ..self.clone()
        }
    }

    async fn token(&self, scope: &str) -> Result<String> {
        #[cfg(test)]
        if self.test_origin.is_some() {
            return Ok(format!(
                "header.{}.signature",
                URL_SAFE_NO_PAD.encode(br#"{"oid":"11111111-1111-1111-1111-111111111111"}"#)
            ));
        }
        let scopes = [scope];
        let token = tokio::select! {
            _ = self.cancellation.cancelled() => bail!("migration cancelled"),
            result = self.credential.get_token(&scopes) => result.context("DefaultAzureCredential could not obtain an Azure token")?,
        };
        Ok(token.token.secret().to_owned())
    }

    pub async fn request(
        &self,
        method: Method,
        url: &str,
        scope: Option<&str>,
        headers: HeaderMap,
        body: Option<Bytes>,
    ) -> Result<Response> {
        let parsed = Url::parse(url).context("invalid Azure URL")?;
        ensure!(
            parsed.scheme() == "https"
                && parsed.username().is_empty()
                && parsed.password().is_none(),
            "Azure URLs must use HTTPS without user credentials"
        );
        #[cfg(test)]
        let parsed = if let Some(origin) = &self.test_origin {
            Url::parse(&format!(
                "{origin}{}{}",
                parsed.path(),
                parsed.query().map(|q| format!("?{q}")).unwrap_or_default()
            ))?
        } else {
            parsed
        };
        for attempt in 0..5_u32 {
            let mut request = self
                .client
                .request(method.clone(), parsed.clone())
                .headers(headers.clone());
            if let Some(scope) = scope {
                request = request.bearer_auth(self.token(scope).await?);
            }
            if let Some(body) = &body {
                request = request.body(body.clone());
            }
            let result = tokio::select! {
                _ = self.cancellation.cancelled() => bail!("migration cancelled"),
                result = tokio::time::timeout(Duration::from_secs(60), request.send()) => result,
            };
            let delay = match result {
                Ok(Ok(response)) => {
                    if !retryable(response.status()) || attempt == 4 {
                        return Ok(response);
                    }
                    retry_after(response.headers())?.unwrap_or_else(|| backoff(attempt))
                }
                Ok(Err(error))
                    if (error.is_timeout() || error.is_connect() || error.is_request())
                        && attempt < 4 =>
                {
                    backoff(attempt)
                }
                Ok(Err(error)) => {
                    return Err(error.without_url()).context("Azure HTTP request failed");
                }
                Err(_) if attempt < 4 => backoff(attempt),
                Err(error) => return Err(error).context("Azure HTTP request timed out"),
            };
            tracing::warn!(
                attempt = attempt + 1,
                retry_seconds = delay.as_secs_f64(),
                "retrying transient Azure request failure"
            );
            self.sleep(delay).await?;
        }
        unreachable!("retry loop returns on its final attempt")
    }

    pub async fn sleep(&self, delay: Duration) -> Result<()> {
        tokio::select! {
            _ = self.cancellation.cancelled() => bail!("migration cancelled"),
            _ = tokio::time::sleep(delay) => Ok(()),
        }
    }

    pub async fn json(response: Response) -> Result<Value> {
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(part) = stream.next().await {
            let part = part.map_err(reqwest::Error::without_url)?;
            ensure!(
                bytes.len() + part.len() <= 2_000_000,
                "Azure JSON response exceeds 2 MB"
            );
            bytes.extend_from_slice(&part);
        }
        serde_json::from_slice(&bytes).context("invalid JSON in Azure response")
    }

    pub async fn expect(response: Response, allowed: &[StatusCode]) -> Result<Response> {
        if allowed.contains(&response.status()) {
            return Ok(response);
        }
        let status = response.status();
        let request_id = response
            .headers()
            .get("x-ms-request-id")
            .and_then(|s| s.to_str().ok())
            .unwrap_or("unavailable")
            .to_owned();
        let body = Self::json(response).await;
        let code = body
            .as_ref()
            .ok()
            .and_then(|v| v.pointer("/error/code"))
            .and_then(Value::as_str)
            .unwrap_or("unavailable");
        bail!(
            "Azure returned HTTP {status} (code: {code}, request ID: {request_id}); check permissions, configuration and Azure service diagnostics"
        )
    }

    async fn arm(
        &self,
        method: Method,
        id: &str,
        api: &str,
        body: Option<Value>,
    ) -> Result<Response> {
        let url = format!("https://management.azure.com{id}?api-version={api}");
        self.request(
            method,
            &url,
            Some(ARM_SCOPE),
            json_headers(),
            body.map(|body| serde_json::to_vec(&body).map(Bytes::from))
                .transpose()?,
        )
        .await
    }

    async fn get_resource(&self, id: &str, api: &str) -> Result<Option<Value>> {
        let response = self.arm(Method::GET, id, api, None).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(
            Self::json(Self::expect(response, &[StatusCode::OK]).await?).await?,
        ))
    }

    async fn ensure_resource(
        &self,
        config: &Config,
        id: &str,
        api: &str,
        body: Value,
    ) -> Result<Value> {
        if let Some(resource) = self.get_resource(id, api).await? {
            return self.await_resource(id, api, Some(resource)).await;
        }
        ensure!(
            config.azure.provision,
            "required resource {id} does not exist and azure.provision is false"
        );
        if let Some((_, provider)) = id.rsplit_once("/providers/") {
            let provider = provider
                .split('/')
                .next()
                .context("invalid resource provider ID")?;
            if [
                "Microsoft.Storage",
                "Microsoft.OperationalInsights",
                "Microsoft.Insights",
                "Microsoft.EventHub",
            ]
            .contains(&provider)
            {
                self.ensure_provider(config, provider).await?;
            }
        }
        tracing::info!(resource = id, "creating Azure resource");
        let response = self.arm(Method::PUT, id, api, Some(body)).await?;
        if response.status() == StatusCode::CONFLICT
            && let Some(resource) = self.get_resource(id, api).await?
        {
            return self.await_resource(id, api, Some(resource)).await;
        }
        Self::expect(
            response,
            &[StatusCode::OK, StatusCode::CREATED, StatusCode::ACCEPTED],
        )
        .await?;
        self.await_resource(id, api, None).await
    }

    async fn ensure_provider(&self, config: &Config, provider: &str) -> Result<()> {
        let mut registered = self.providers.lock().await;
        if registered.contains(provider) {
            return Ok(());
        }
        let id = format!(
            "/subscriptions/{}/providers/{provider}",
            config.azure.subscription_id
        );
        let api = "2021-04-01";
        let mut resource = self
            .get_resource(&id, api)
            .await?
            .context("Azure resource provider was not found")?;
        if resource["registrationState"] != "Registered" {
            Self::expect(
                self.arm(Method::POST, &format!("{id}/register"), api, None)
                    .await?,
                &[StatusCode::OK, StatusCode::ACCEPTED],
            )
            .await?;
            let deadline = Instant::now() + Duration::from_secs(600);
            while resource["registrationState"] != "Registered" {
                ensure!(
                    Instant::now() < deadline,
                    "timed out registering {provider}"
                );
                self.sleep(Duration::from_secs(2)).await?;
                resource = self
                    .get_resource(&id, api)
                    .await?
                    .context("Azure resource provider disappeared")?;
            }
        }
        registered.insert(provider.to_owned());
        Ok(())
    }

    async fn await_resource(
        &self,
        id: &str,
        api: &str,
        mut current: Option<Value>,
    ) -> Result<Value> {
        let deadline = Instant::now() + Duration::from_secs(900);
        if current.is_none() {
            current = self.get_resource(id, api).await?;
        }
        loop {
            if let Some(resource) = current.take() {
                match resource
                    .pointer("/properties/provisioningState")
                    .and_then(Value::as_str)
                {
                    Some("Failed" | "Canceled" | "Cancelled") => {
                        bail!("provisioning failed for {id}")
                    }
                    Some("Succeeded") | None => return Ok(resource),
                    Some(_) => {}
                }
            }
            ensure!(Instant::now() < deadline, "timed out provisioning {id}");
            self.sleep(Duration::from_secs(2)).await?;
            current = self.get_resource(id, api).await?;
        }
    }

    async fn principal_id(&self, config: &Config) -> Result<Uuid> {
        if let Some(id) = config.azure.principal_id {
            return Ok(id);
        }
        let token = self.token(ARM_SCOPE).await?;
        let payload = token
            .split('.')
            .nth(1)
            .context("ARM token is not a JWT; configure azure.principal_id explicitly")?;
        let payload = URL_SAFE_NO_PAD
            .decode(payload.trim_end_matches('='))
            .context("decoding ARM token claims")?;
        let claims: Value = serde_json::from_slice(&payload)?;
        claims["oid"]
            .as_str()
            .context("ARM token has no oid; configure azure.principal_id")?
            .parse()
            .context("ARM token oid is not a UUID")
    }

    async fn ensure_role(
        &self,
        config: &Config,
        scope: &str,
        principal: Uuid,
        role: &str,
        principal_type: Option<&str>,
    ) -> Result<()> {
        if !config.azure.provision {
            return Ok(());
        }
        let name = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("{scope}/{principal}/{role}").as_bytes(),
        );
        let id = format!("{scope}/providers/Microsoft.Authorization/roleAssignments/{name}");
        if self.get_resource(&id, ROLE_API).await?.is_some() {
            return Ok(());
        }
        let mut properties = json!({
                "principalId": principal,
                "roleDefinitionId": format!("/subscriptions/{}/providers/Microsoft.Authorization/roleDefinitions/{role}", config.azure.subscription_id),
        });
        if let Some(kind) = principal_type {
            properties["principalType"] = Value::String(kind.into());
        }
        let response = self
            .arm(
                Method::PUT,
                &id,
                ROLE_API,
                Some(json!({"properties": properties})),
            )
            .await?;
        if response.status() == StatusCode::CONFLICT {
            let error = Self::json(response).await?;
            ensure!(
                error.pointer("/error/code").and_then(Value::as_str)
                    == Some("RoleAssignmentExists"),
                "Azure rejected role assignment with a conflict: {}",
                error
                    .pointer("/error/code")
                    .and_then(Value::as_str)
                    .unwrap_or("unavailable")
            );
        } else {
            Self::expect(response, &[StatusCode::OK, StatusCode::CREATED]).await?;
        }
        Ok(())
    }

    pub async fn ensure_state_resources(&self, config: &Config) -> Result<()> {
        self.ensure_resource(
            config,
            &config.group_id(),
            "2022-09-01",
            json!({"location": config.azure.location}),
        )
        .await?;
        let storage = config.resource_id(
            "Microsoft.Storage",
            "storageAccounts",
            &config.azure.storage_account,
        );
        self.ensure_resource(
            config,
            &storage,
            STORAGE_API,
            json!({
                "location": config.azure.location,
                "kind": "StorageV2",
                "sku": {"name": "Standard_LRS"},
                "properties": {
                    "supportsHttpsTrafficOnly": true, "minimumTlsVersion": "TLS1_2",
                    "allowBlobPublicAccess": false, "allowSharedKeyAccess": false,
                }
            }),
        )
        .await?;
        self.ensure_resource(
            config,
            &format!("{storage}/tableServices/default/tables/{STATE_TABLE}"),
            STORAGE_API,
            json!({"properties": {}}),
        )
        .await?;
        if config.azure.provision {
            self.ensure_role(
                config,
                &storage,
                self.principal_id(config).await?,
                TABLE_CONTRIBUTOR,
                None,
            )
            .await?;
        }
        Ok(())
    }

    pub async fn ensure_target(&self, config: &Config) -> Result<Sink> {
        let workspace_id = config.resource_id(
            "Microsoft.OperationalInsights",
            "workspaces",
            &config.azure.workspace,
        );
        let existing = self.get_resource(&workspace_id, WORKSPACE_API).await?;
        if matches!(config.target, Target::EventHub { .. }) && existing.is_none() {
            ensure!(
                config.azure.workspace_capacity_gb.is_some(),
                "Event Hub-to-DCR ingestion requires a commitment-tier or dedicated-cluster workspace; set workspace_capacity_gb explicitly to create a paid commitment tier"
            );
        }
        let sku = match config.azure.workspace_capacity_gb {
            Some(gb) => json!({"name": "CapacityReservation", "capacityReservationLevel": gb}),
            None => json!({"name": "PerGB2018"}),
        };
        let workspace = if let Some(existing) = existing {
            self.await_resource(&workspace_id, WORKSPACE_API, Some(existing))
                .await?
        } else {
            self.ensure_resource(
                config,
                &workspace_id,
                WORKSPACE_API,
                json!({
                    "location": config.azure.location,
                    "properties": {"sku": sku, "retentionInDays": 30},
                }),
            )
            .await?
        };
        check_location(&workspace, &config.azure.location)?;
        if matches!(config.target, Target::EventHub { .. })
            && workspace
                .pointer("/properties/sku/name")
                .and_then(Value::as_str)
                != Some("CapacityReservation")
        {
            let linked = self
                .get_resource(
                    &format!("{workspace_id}/linkedServices/cluster"),
                    "2020-08-01",
                )
                .await?;
            ensure!(
                linked
                    .as_ref()
                    .and_then(|v| v.pointer("/properties/resourceId"))
                    .and_then(Value::as_str)
                    .is_some(),
                "native Event Hub-to-DCR ingestion requires an existing commitment-tier or dedicated-cluster workspace"
            );
        }
        let table = self.ensure_resource(config, &format!("{workspace_id}/tables/{}", config.azure.table),
            "2022-10-01", json!({"properties": {"schema": {"name": config.azure.table, "columns": table_columns()}}})).await?;
        let schema = table
            .pointer("/properties/schema")
            .context("log table has no schema")?;
        let mut table_columns = Vec::new();
        for name in ["columns", "standardColumns"] {
            if let Some(value) = schema.get(name) {
                table_columns.extend(
                    value
                        .as_array()
                        .context("invalid log table column schema")?
                        .iter()
                        .cloned(),
                );
            }
        }
        ensure_columns(Some(&Value::Array(table_columns)), &columns())?;

        let endpoint_id = config.resource_id(
            "Microsoft.Insights",
            "dataCollectionEndpoints",
            &format!("{}-endpoint", config.azure.dcr),
        );
        let endpoint = self
            .ensure_resource(
                config,
                &endpoint_id,
                MONITOR_API,
                json!({
                    "location": config.azure.location,
                    "properties": {"networkAcls": {"publicNetworkAccess": "Enabled"}},
                }),
            )
            .await?;
        check_location(&endpoint, &config.azure.location)?;
        let dcr_id = config.resource_id(
            "Microsoft.Insights",
            "dataCollectionRules",
            &config.azure.dcr,
        );
        let stream = "Custom-GotomochCosts";
        let event_columns = json!([
            {"name": "TimeGenerated", "type": "datetime"},
            {"name": "RawData", "type": "string"},
            {"name": "Properties", "type": "dynamic"},
        ]);
        let mut properties = json!({
            "dataCollectionEndpointId": endpoint_id,
            "streamDeclarations": {stream: {"columns": columns()}},
            "destinations": {"logAnalytics": [{"workspaceResourceId": workspace_id, "name": "costs"}]},
            "dataFlows": [{
                "streams": [stream], "destinations": ["costs"], "transformKql": "source",
                "outputStream": format!("Custom-{}", config.azure.table),
            }],
        });
        let mut event_hub_id = None;
        if let Target::EventHub {
            namespace,
            hub,
            consumer_group,
        } = &config.target
        {
            let namespace_id = config.resource_id("Microsoft.EventHub", "namespaces", namespace);
            let namespace_resource = self.ensure_resource(config, &namespace_id, EVENT_HUB_API, json!({
                "location": config.azure.location, "sku": {"name": "Standard", "tier": "Standard", "capacity": 1},
                "properties": {"minimumTlsVersion": "1.2", "disableLocalAuth": true},
            })).await?;
            check_location(&namespace_resource, &config.azure.location)?;
            let hub_id = format!("{namespace_id}/eventhubs/{hub}");
            self.ensure_resource(
                config,
                &hub_id,
                EVENT_HUB_API,
                json!({
                    "properties": {"messageRetentionInDays": 7, "partitionCount": 2},
                }),
            )
            .await?;
            self.ensure_resource(
                config,
                &format!("{hub_id}/consumerGroups/{consumer_group}"),
                EVENT_HUB_API,
                json!({"properties": {}}),
            )
            .await?;
            properties["streamDeclarations"][stream]["columns"] = event_columns;
            properties["dataSources"] = json!({"dataImports": {"eventHub": {
                "name": "costs", "consumerGroup": consumer_group, "stream": stream,
            }}});
            properties["dataFlows"][0]["transformKql"] = Value::String(event_transform());
            event_hub_id = Some(hub_id);
        }
        let dcr = self
            .ensure_resource(
                config,
                &dcr_id,
                MONITOR_API,
                json!({
                    "location": config.azure.location,
                    "identity": {"type": "SystemAssigned"}, "properties": properties,
                }),
            )
            .await?;
        check_location(&dcr, &config.azure.location)?;
        validate_dcr(&dcr, &properties)?;
        let principal = if config.azure.provision {
            Some(self.principal_id(config).await?)
        } else {
            None
        };
        match &config.target {
            Target::Direct => {
                if let Some(principal) = principal {
                    self.ensure_role(config, &dcr_id, principal, METRICS_PUBLISHER, None)
                        .await?;
                }
                let base = endpoint
                    .pointer("/properties/logsIngestion/endpoint")
                    .and_then(Value::as_str)
                    .context("DCE has no logsIngestion endpoint")?;
                let base = trusted_monitor_url(base)?;
                let immutable = dcr
                    .pointer("/properties/immutableId")
                    .and_then(Value::as_str)
                    .context("DCR has no immutableId")?;
                ensure!(
                    immutable
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'-'),
                    "invalid DCR immutableId"
                );
                Ok(Sink {
                    url: format!(
                        "{}/dataCollectionRules/{immutable}/streams/{stream}?api-version=2023-01-01",
                        base.trim_end_matches('/')
                    ),
                    audience: MONITOR_SCOPE,
                    content_type: "application/json",
                    success: StatusCode::NO_CONTENT,
                    propagation_deadline: config
                        .azure
                        .provision
                        .then(|| Instant::now() + Duration::from_secs(180)),
                })
            }
            Target::EventHub { namespace, hub, .. } => {
                let hub_id = event_hub_id.context("missing Event Hub resource ID")?;
                if let Some(principal) = principal {
                    self.ensure_role(config, &hub_id, principal, EVENT_SENDER, None)
                        .await?;
                    let dcr_principal: Uuid = dcr
                        .pointer("/identity/principalId")
                        .and_then(Value::as_str)
                        .context("DCR has no managed identity")?
                        .parse()?;
                    self.ensure_role(
                        config,
                        &hub_id,
                        dcr_principal,
                        EVENT_RECEIVER,
                        Some("ServicePrincipal"),
                    )
                    .await?;
                }
                let association = self.ensure_resource(config,
                    &format!("{hub_id}/providers/Microsoft.Insights/dataCollectionRuleAssociations/gotomoch"),
                    "2022-06-01", json!({"properties": {"dataCollectionRuleId": dcr_id}})).await?;
                ensure!(
                    association.pointer("/properties/dataCollectionRuleId")
                        == Some(&Value::String(dcr_id)),
                    "Event Hub association points at a different DCR"
                );
                Ok(Sink {
                    url: format!(
                        "https://{namespace}.servicebus.windows.net/{hub}/messages?api-version=2014-01"
                    ),
                    audience: EVENT_HUB_SCOPE,
                    content_type: "application/vnd.microsoft.servicebus.json",
                    success: StatusCode::CREATED,
                    propagation_deadline: config
                        .azure
                        .provision
                        .then(|| Instant::now() + Duration::from_secs(180)),
                })
            }
        }
    }

    pub async fn send_batch(&self, sink: &Sink, batch: &Batch) -> Result<()> {
        let mut headers = json_headers();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(sink.content_type),
        );
        loop {
            let response = self
                .request(
                    Method::POST,
                    &sink.url,
                    Some(sink.audience),
                    headers.clone(),
                    Some(batch.body.clone()),
                )
                .await?;
            if response.status() == StatusCode::FORBIDDEN
                && sink
                    .propagation_deadline
                    .is_some_and(|deadline| Instant::now() < deadline)
            {
                tracing::info!("waiting for ingestion RBAC propagation");
                self.sleep(Duration::from_secs(5)).await?;
                continue;
            }
            Self::expect(response, &[sink.success]).await?;
            return Ok(());
        }
    }

    pub async fn start_report(&self, config: &Config, day: NaiveDate) -> Result<ReportStart> {
        let response = self
            .arm(
                Method::POST,
                &format!(
                    "{}/providers/Microsoft.CostManagement/generateCostDetailsReport",
                    config.job.scope
                ),
                COST_API,
                Some(
                    json!({"metric": config.job.metric, "timePeriod": {"start": day, "end": day}}),
                ),
            )
            .await?;
        let status = response.status();
        if status == StatusCode::NO_CONTENT {
            return Ok(ReportStart::Empty);
        }
        let response = Self::expect(response, &[StatusCode::OK, StatusCode::ACCEPTED]).await?;
        let location = response
            .headers()
            .get(header::LOCATION)
            .map(|v| v.to_str().map(str::to_owned))
            .transpose()?;
        if status == StatusCode::ACCEPTED {
            let operation = trusted_arm_url(
                location
                    .as_deref()
                    .context("report request has no Location header")?,
            )?;
            return Ok(ReportStart::Pending {
                operation,
                delay: retry_after(response.headers())?.unwrap_or(Duration::from_secs(5)),
            });
        }
        let result = Self::json(response).await?;
        if result["status"] == "NoDataFound" {
            return Ok(ReportStart::Empty);
        }
        let operation = operation_url(
            location
                .as_deref()
                .or(result["id"].as_str())
                .context("completed report has no operation ID or Location")?,
        )?;
        Ok(ReportStart::Ready {
            operation,
            report: parse_report(&result)?,
        })
    }

    pub async fn poll_report(&self, operation: &str) -> Result<ReportPoll> {
        let operation = trusted_arm_url(operation)?;
        let response = self
            .request(
                Method::GET,
                &operation,
                Some(ARM_SCOPE),
                HeaderMap::new(),
                None,
            )
            .await?;
        let response = Self::expect(response, &[StatusCode::OK, StatusCode::ACCEPTED]).await?;
        if response.status() == StatusCode::ACCEPTED {
            return Ok(ReportPoll::Pending(
                retry_after(response.headers())?.unwrap_or(Duration::from_secs(5)),
            ));
        }
        let result = Self::json(response).await?;
        if result["status"] == "NoDataFound" {
            Ok(ReportPoll::Empty)
        } else {
            Ok(ReportPoll::Ready(parse_report(&result)?))
        }
    }

    pub async fn open_blob(
        &self,
        blob: &Blob,
        compressed: bool,
        mut cursor: Cursor,
        config: &Config,
    ) -> Result<CsvSource> {
        let url = Url::parse(&blob.blob_link).context("invalid report blob URL")?;
        ensure!(
            url.scheme() == "https"
                && url
                    .host_str()
                    .is_some_and(|h| h.ends_with(".blob.core.windows.net")),
            "cost report must reference a public-cloud Azure Blob HTTPS URL"
        );
        let compressed = blob.is_gzip(compressed)?;
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("identity"),
        );
        if let Some(etag) = &cursor.etag {
            headers.insert(header::IF_MATCH, HeaderValue::from_str(etag)?);
        }
        if cursor.offset > 0 && !compressed {
            headers.insert(
                header::RANGE,
                HeaderValue::from_str(&format!("bytes={}-", cursor.offset))?,
            );
        }
        let response = self
            .request(Method::GET, &blob.blob_link, None, headers, None)
            .await?;
        let response =
            Self::expect(response, &[StatusCode::OK, StatusCode::PARTIAL_CONTENT]).await?;
        if cursor.offset > 0 && !compressed {
            ensure!(
                response.status() == StatusCode::PARTIAL_CONTENT,
                "blob server ignored the resume Range request"
            );
            let range = response
                .headers()
                .get(header::CONTENT_RANGE)
                .context("resumed blob has no Content-Range")?
                .to_str()?;
            ensure!(
                range.starts_with(&format!("bytes {}-", cursor.offset)),
                "blob Content-Range does not match the checkpoint"
            );
        }
        let etag = response
            .headers()
            .get(header::ETAG)
            .context("report blob has no ETag")?
            .to_str()?
            .to_owned();
        ensure!(
            cursor.etag.as_ref().is_none_or(|saved| saved == &etag),
            "report blob ETag changed; refusing unsafe resume"
        );
        cursor.etag = Some(etag);
        let stream = response
            .bytes_stream()
            .map_err(|error| io::Error::other(error.without_url()));
        let input = StreamReader::new(stream);
        let mut input: Input = if compressed {
            Box::pin(GzipDecoder::new(BufReader::new(input)))
        } else {
            Box::pin(input)
        };
        if compressed && cursor.offset > 0 {
            let skipped = tokio::io::copy(
                &mut (&mut input).take(cursor.offset),
                &mut tokio::io::sink(),
            )
            .await?;
            ensure!(
                skipped == cursor.offset,
                "compressed source ended before the saved CSV offset"
            );
        }
        CsvSource::new(input, cursor, config).await
    }

    pub async fn table_request(
        &self,
        config: &Config,
        method: Method,
        entity: bool,
        etag: Option<&str>,
        body: Option<Value>,
    ) -> Result<Response> {
        let suffix = if entity {
            format!("(PartitionKey='jobs',RowKey='{}')", config.job.id)
        } else {
            String::new()
        };
        let url = format!(
            "https://{}.table.core.windows.net/{STATE_TABLE}{suffix}",
            config.azure.storage_account
        );
        let mut headers = json_headers();
        headers.insert("x-ms-version", HeaderValue::from_static("2019-02-02"));
        headers.insert(
            "x-ms-date",
            HeaderValue::from_str(&httpdate::fmt_http_date(SystemTime::now()))?,
        );
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/json;odata=nometadata"),
        );
        headers.insert("Prefer", HeaderValue::from_static("return-content"));
        if let Some(etag) = etag {
            headers.insert(header::IF_MATCH, HeaderValue::from_str(etag)?);
        }
        self.request(
            method,
            &url,
            Some(STORAGE_SCOPE),
            headers,
            body.map(|value| serde_json::to_vec(&value).map(Bytes::from))
                .transpose()?,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) fn for_test(origin: String) -> Self {
        let mut azure = Self::new(CancellationToken::new()).unwrap();
        azure.test_origin = Some(origin);
        azure
    }
}

pub fn columns() -> Value {
    json!([
        {"name": "TimeGenerated", "type": "datetime"},
        {"name": "CostDate", "type": "datetime"},
        {"name": "Currency", "type": "string"},
        {"name": "Metric", "type": "string"},
        {"name": "Dimensions", "type": "dynamic"},
        {"name": "Cost", "type": "real"},
        {"name": "Rows", "type": "long"},
        {"name": "RecordId", "type": "string"},
        {"name": "JobId", "type": "string"},
    ])
}

fn table_columns() -> Value {
    let mut columns = columns();
    for column in columns.as_array_mut().expect("static column array") {
        if column["type"] == "datetime" {
            column["type"] = json!("dateTime");
        }
    }
    columns
}

fn ensure_columns(actual: Option<&Value>, expected: &Value) -> Result<()> {
    let actual = actual
        .and_then(Value::as_array)
        .context("resource has no column schema")?;
    for column in expected.as_array().context("invalid expected schema")? {
        ensure!(
            actual.iter().any(|v| v["name"] == column["name"]
                && v["type"]
                    .as_str()
                    .zip(column["type"].as_str())
                    .is_some_and(|(a, b)| a.eq_ignore_ascii_case(b))),
            "existing schema is missing column {} with type {}",
            column["name"],
            column["type"]
        );
    }
    Ok(())
}

fn validate_dcr(dcr: &Value, expected: &Value) -> Result<()> {
    let actual = &dcr["properties"];
    for name in [
        "dataCollectionEndpointId",
        "destinations",
        "dataFlows",
        "dataSources",
    ] {
        let matches = match expected.get(name) {
            Some(expected) => actual
                .get(name)
                .is_some_and(|actual| contract_matches(actual, expected)),
            None => actual
                .get(name)
                .is_none_or(|v| v.is_null() || v.as_object().is_some_and(|v| v.is_empty())),
        };
        ensure!(
            matches,
            "existing DCR {name} differs from this job; use a dedicated DCR or correct its configuration"
        );
    }
    for flow in actual["dataFlows"]
        .as_array()
        .context("DCR has no dataFlows")?
    {
        ensure!(
            flow.get("builtInTransform")
                .is_none_or(|v| v.is_null() || v.as_str() == Some("")),
            "existing DCR has an unexpected built-in transform"
        );
        ensure!(
            flow.get("captureOverflow")
                .is_none_or(|v| v.is_null() || v.as_bool() == Some(false)),
            "existing DCR has an unexpected overflow transform"
        );
    }
    ensure_columns(
        actual.pointer("/streamDeclarations/Custom-GotomochCosts/columns"),
        &expected["streamDeclarations"]["Custom-GotomochCosts"]["columns"],
    )
}

fn contract_matches(actual: &Value, expected: &Value) -> bool {
    match expected {
        Value::Object(fields) => fields.iter().all(|(key, value)| {
            actual
                .get(key)
                .is_some_and(|actual| contract_matches(actual, value))
        }),
        Value::Array(expected) => actual.as_array().is_some_and(|actual| {
            actual.len() == expected.len()
                && actual
                    .iter()
                    .zip(expected)
                    .all(|(a, b)| contract_matches(a, b))
        }),
        Value::String(id) if id.starts_with("/subscriptions/") => actual
            .as_str()
            .is_some_and(|actual| actual.eq_ignore_ascii_case(id)),
        _ => actual == expected,
    }
}

fn check_location(resource: &Value, expected: &str) -> Result<()> {
    ensure!(
        resource["location"]
            .as_str()
            .is_some_and(|v| normalize_location(v) == normalize_location(expected)),
        "existing Azure resource is not in configured location {expected}"
    );
    Ok(())
}

fn normalize_location(value: &str) -> String {
    value
        .chars()
        .filter(|c| !c.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

fn event_transform() -> String {
    "source | extend data = parse_json(RawData) | project TimeGenerated = todatetime(data.TimeGenerated), CostDate = todatetime(data.CostDate), Currency = tostring(data.Currency), Metric = tostring(data.Metric), Dimensions = data.Dimensions, Cost = toreal(data.Cost), Rows = tolong(data.Rows), RecordId = tostring(data.RecordId), JobId = tostring(data.JobId)".into()
}

fn parse_report(result: &Value) -> Result<Report> {
    ensure!(
        result["status"] == "Completed",
        "cost report failed or returned an unknown status; Azure error code: {}",
        result
            .pointer("/error/code")
            .and_then(Value::as_str)
            .unwrap_or("unavailable")
    );
    let manifest = result
        .get("manifest")
        .context("completed report has no manifest")?;
    ensure!(
        manifest["dataFormat"] == "Csv",
        "cost report format is not CSV"
    );
    let blobs: Vec<Blob> = serde_json::from_value(manifest["blobs"].clone())
        .context("invalid cost report blob manifest")?;
    if let Some(count) = manifest.get("blobCount") {
        let count = count.as_u64().context("invalid report blobCount")?;
        ensure!(
            count == blobs.len() as u64,
            "cost report blobCount does not match its manifest"
        );
    }
    let valid_till: Option<DateTime<Utc>> = result
        .get("validTill")
        .filter(|v| !v.is_null())
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()?;
    if let Some(expiry) = valid_till {
        ensure!(
            expiry > Utc::now(),
            "saved cost report has expired; cannot safely regenerate a partly acknowledged report"
        );
    }
    let compressed = manifest
        .get("compressData")
        .map(|v| v.as_bool().context("invalid compressData flag"))
        .transpose()?
        .unwrap_or(false);
    Ok(Report {
        blobs,
        compressed,
        valid_till,
    })
}

fn trusted_arm_url(value: &str) -> Result<String> {
    let url = Url::parse(value).context("invalid Azure operation URL")?;
    ensure!(
        url.scheme() == "https"
            && url.host_str() == Some("management.azure.com")
            && url.port_or_known_default() == Some(443)
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none(),
        "untrusted Azure operation URL"
    );
    Ok(url.into())
}

fn operation_url(value: &str) -> Result<String> {
    if value.starts_with('/') {
        trusted_arm_url(&format!(
            "https://management.azure.com{value}?api-version={COST_API}"
        ))
    } else {
        trusted_arm_url(value)
    }
}

fn trusted_monitor_url(value: &str) -> Result<String> {
    let url = Url::parse(value)?;
    ensure!(
        url.scheme() == "https"
            && url
                .host_str()
                .is_some_and(|h| h.ends_with(".ingest.monitor.azure.com"))
            && url.port_or_known_default() == Some(443)
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "untrusted Azure Monitor ingestion endpoint"
    );
    Ok(url.into())
}

pub fn retry_after(headers: &HeaderMap) -> Result<Option<Duration>> {
    let mut delay = None;
    for name in [
        "retry-after",
        "x-ms-ratelimit-microsoft.costmanagement-qpu-retry-after",
        "x-ms-ratelimit-microsoft.costmanagement-entity-retry-after",
        "x-ms-ratelimit-microsoft.costmanagement-clienttype-retry-after",
    ] {
        if let Some(header) = headers.get(name) {
            let value = header.to_str()?;
            let duration = if let Ok(seconds) = value.parse::<u64>() {
                Duration::from_secs(seconds)
            } else {
                httpdate::parse_http_date(value)
                    .context("invalid Azure Retry-After header")?
                    .duration_since(SystemTime::now())
                    .unwrap_or(Duration::ZERO)
            };
            delay = Some(delay.map_or(duration, |current: Duration| current.max(duration)));
        }
    }
    Ok(delay)
}

fn backoff(attempt: u32) -> Duration {
    #[cfg(test)]
    {
        let _ = attempt;
        Duration::from_millis(1)
    }
    #[cfg(not(test))]
    {
        Duration::from_millis((1_u64 << attempt) * 1_000 + fastrand::u64(0..250))
    }
}

fn retryable(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

fn json_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers
}
