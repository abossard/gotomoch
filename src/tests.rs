use crate::{
    azure::{Azure, STATE_TABLE, columns, retry_after},
    config::{Config, Target},
    pipeline::Cursor,
    runner,
    state::{self, Event, Lease, Pending, Phase, State},
};
use chrono::{NaiveDate, Utc};
use reqwest::{Method, header::HeaderMap};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::AsyncReadExt;
use wiremock::{Mock, MockServer, Request, ResponseTemplate, matchers::any};

const OPERATION: &str = "/subscriptions/00000000-0000-0000-0000-000000000000/providers/Microsoft.CostManagement/costDetailsOperationResults/report1";

#[derive(Default)]
struct Cloud {
    resources: BTreeMap<String, Value>,
    checkpoint: Option<Value>,
    etag: u64,
    csv: Vec<u8>,
    compressed: bool,
    bodies: Vec<Value>,
    ranges: Vec<Option<u64>>,
    report_requests: usize,
    requests: Vec<(String, String)>,
    fail_after: Option<usize>,
    throttle: usize,
    empty: bool,
}

impl Cloud {
    fn respond(&mut self, request: &Request) -> ResponseTemplate {
        let path = request.url.path();
        let method = request.method.as_str();
        self.requests.push((method.into(), path.into()));
        if path.starts_with(&format!("/{STATE_TABLE}")) {
            assert!(request.headers.contains_key("x-ms-date"));
            match method {
                "GET" => {
                    return match &self.checkpoint {
                        Some(value) => self.entity_response(200).set_body_json(value),
                        None => ResponseTemplate::new(404),
                    };
                }
                "POST" => {
                    if self.checkpoint.is_some() {
                        return ResponseTemplate::new(409);
                    }
                    self.checkpoint = Some(serde_json::from_slice(&request.body).unwrap());
                    self.etag += 1;
                    return self.entity_response(201);
                }
                "PUT" => {
                    let expected = format!("\"{}\"", self.etag);
                    if request.headers.get("if-match").map(|v| v.to_str().unwrap())
                        != Some(expected.as_str())
                    {
                        return ResponseTemplate::new(412);
                    }
                    self.checkpoint = Some(serde_json::from_slice(&request.body).unwrap());
                    self.etag += 1;
                    return self.entity_response(204);
                }
                _ => panic!("unexpected table operation"),
            }
        }
        if path.ends_with("/generateCostDetailsReport") && method == "POST" {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["timePeriod"]["start"], body["timePeriod"]["end"]);
            self.report_requests += 1;
            if self.empty {
                return ResponseTemplate::new(204);
            }
            return ResponseTemplate::new(202)
                .insert_header(
                    "location",
                    format!("https://management.azure.com{OPERATION}?api-version=2024-08-01"),
                )
                .insert_header("retry-after", "0");
        }
        if path == OPERATION {
            return ResponseTemplate::new(200).set_body_json(json!({
                "status": "Completed", "validTill": (Utc::now() + chrono::Duration::hours(24)).to_rfc3339(),
                "manifest": {"dataFormat": "Csv", "blobCount": 1, "compressData": self.compressed,
                    "blobs": [{"blobLink": "https://reports.blob.core.windows.net/report.csv?sig=not-a-secret", "byteCount": self.csv.len()}]},
            }));
        }
        if path == "/report.csv" {
            assert!(
                !request.headers.contains_key("authorization"),
                "must not forward Azure bearer tokens to SAS blobs"
            );
            let range = request.headers.get("range").map(|v| {
                v.to_str()
                    .unwrap()
                    .strip_prefix("bytes=")
                    .unwrap()
                    .trim_end_matches('-')
                    .parse::<u64>()
                    .unwrap()
            });
            self.ranges.push(range);
            if let Some(etag) = request.headers.get("if-match")
                && etag != "\"blob1\""
            {
                return ResponseTemplate::new(412);
            }
            let start = range.unwrap_or(0) as usize;
            if start >= self.csv.len() {
                return ResponseTemplate::new(416);
            }
            let mut response = ResponseTemplate::new(if range.is_some() { 206 } else { 200 })
                .insert_header("etag", "\"blob1\"")
                .set_body_bytes(self.csv[start..].to_vec());
            if range.is_some() {
                response = response.insert_header(
                    "content-range",
                    format!("bytes {start}-{}/{}", self.csv.len() - 1, self.csv.len()),
                );
            }
            return response;
        }
        if method == "POST"
            && (path.contains("/dataCollectionRules/") || path.ends_with("/messages"))
        {
            if self.throttle > 0 {
                self.throttle -= 1;
                return ResponseTemplate::new(429).insert_header("retry-after", "0");
            }
            if self.fail_after.is_some_and(|n| self.bodies.len() >= n) {
                return ResponseTemplate::new(400)
                    .set_body_json(json!({"error": {"code": "InvalidPayload"}}));
            }
            self.bodies
                .push(serde_json::from_slice(&request.body).unwrap());
            return ResponseTemplate::new(if path.ends_with("/messages") {
                201
            } else {
                204
            });
        }
        if method == "GET"
            && [
                "/providers/Microsoft.Storage",
                "/providers/Microsoft.OperationalInsights",
                "/providers/Microsoft.Insights",
                "/providers/Microsoft.EventHub",
            ]
            .iter()
            .any(|p| path.ends_with(p))
        {
            return ResponseTemplate::new(200)
                .set_body_json(json!({"registrationState": "Registered"}));
        }
        match method {
            "GET" => match self.resources.get(path) {
                Some(value) => ResponseTemplate::new(200).set_body_json(value),
                None => ResponseTemplate::new(404),
            },
            "PUT" => {
                let mut resource: Value = serde_json::from_slice(&request.body).unwrap();
                if path.contains("/dataCollectionEndpoints/") {
                    resource["properties"]["logsIngestion"] =
                        json!({"endpoint": "https://demo.westeurope-1.ingest.monitor.azure.com"});
                }
                if path.contains("/dataCollectionRules/") && !path.contains("/roleAssignments/") {
                    resource["properties"]["immutableId"] = json!("dcr-123");
                    resource["identity"]["principalId"] =
                        json!("22222222-2222-2222-2222-222222222222");
                    resource["properties"]["destinations"]["logAnalytics"][0]["workspaceId"] =
                        json!("33333333-3333-3333-3333-333333333333");
                    resource["properties"]["dataFlows"][0]["captureOverflow"] = json!(false);
                    resource["properties"]["dataFlows"][0]["builtInTransform"] = Value::Null;
                    let endpoint = resource["properties"]["dataCollectionEndpointId"]
                        .as_str()
                        .unwrap()
                        .to_lowercase();
                    resource["properties"]["dataCollectionEndpointId"] = json!(endpoint);
                    if resource["properties"].get("dataSources").is_none() {
                        resource["properties"]["dataSources"] = json!({});
                    }
                }
                if path.contains("/workspaces/") && path.contains("/tables/") {
                    let columns = resource["properties"]["schema"]["columns"]
                        .as_array_mut()
                        .unwrap();
                    let index = columns
                        .iter()
                        .position(|c| c["name"] == "TimeGenerated")
                        .unwrap();
                    let generated = columns.remove(index);
                    resource["properties"]["schema"]["standardColumns"] = json!([generated]);
                }
                self.resources.insert(path.into(), resource);
                ResponseTemplate::new(201)
            }
            _ => panic!("unexpected request: {method} {path}"),
        }
    }

    fn entity_response(&self, status: u16) -> ResponseTemplate {
        ResponseTemplate::new(status).insert_header("etag", format!("\"{}\"", self.etag))
    }

    fn state(&self) -> State {
        serde_json::from_str(self.checkpoint.as_ref().unwrap()["State"].as_str().unwrap()).unwrap()
    }
}

fn config() -> Config {
    let mut config: Config = toml::from_str(include_str!("../examples/direct.toml")).unwrap();
    config.job.start_date = day();
    config
}

fn day() -> NaiveDate {
    static DAY: std::sync::OnceLock<NaiveDate> = std::sync::OnceLock::new();
    *DAY.get_or_init(|| Utc::now().date_naive().pred_opt().unwrap())
}

async fn cloud(mut csv: Vec<u8>) -> (MockServer, Arc<Mutex<Cloud>>, Azure) {
    if csv.starts_with(b"Date,") {
        csv = String::from_utf8(csv)
            .unwrap()
            .replace("2026-10-01", &day().to_string())
            .into_bytes();
    }
    let server = MockServer::start().await;
    let cloud = Arc::new(Mutex::new(Cloud {
        csv,
        ..Cloud::default()
    }));
    let handler = cloud.clone();
    Mock::given(any())
        .respond_with(move |request: &Request| handler.lock().unwrap().respond(request))
        .mount(&server)
        .await;
    let azure = Azure::for_test(server.uri());
    (server, cloud, azure)
}

#[tokio::test]
async fn provisions_runs_and_does_no_source_or_sink_work_when_caught_up() {
    let (_server, cloud, azure) =
        cloud(include_bytes!("../tests/fixtures/costs.csv").to_vec()).await;
    let config = config();
    let result = runner::run(&azure, &config, day()).await.unwrap();
    assert_eq!(result["totals"]["input_rows"], 5);
    assert_eq!(result["totals"]["output_rows"], 3);
    let requests = cloud.lock().unwrap().requests.len();
    let again = runner::run(&azure, &config, day()).await.unwrap();
    assert_eq!(result, again);
    let cloud = cloud.lock().unwrap();
    assert_eq!(cloud.report_requests, 1);
    assert_eq!(cloud.bodies.len(), 1);
    assert_eq!(cloud.ranges, vec![None]);
    assert!(
        cloud.requests[requests..]
            .iter()
            .all(|(_, path)| !path.contains("/workspaces/")
                && !path.contains("/dataCollectionRules/"))
    );
    assert_eq!(cloud.checkpoint.as_ref().unwrap()["Owner"], "");
    let storage = config.resource_id(
        "Microsoft.Storage",
        "storageAccounts",
        &config.azure.storage_account,
    );
    assert_eq!(
        cloud.resources[&storage]["properties"]["allowSharedKeyAccess"],
        false
    );
    let table = config.resource_id(
        "Microsoft.OperationalInsights",
        "workspaces",
        &config.azure.workspace,
    ) + "/tables/AzureCosts_CL";
    let schema = &cloud.resources[&table]["properties"]["schema"];
    assert_eq!(
        schema["columns"].as_array().unwrap().len(),
        columns().as_array().unwrap().len() - 1
    );
    assert_eq!(
        schema["standardColumns"][0],
        json!({"name": "TimeGenerated", "type": "dateTime"})
    );
}

#[tokio::test]
async fn resumes_partial_writes_without_resending_acknowledged_batches() {
    let mut config = config();
    config.limits.batch_bytes = 64_000;
    config.limits.concurrency = 1;
    let mut csv = "Date,Cost,Currency,SubscriptionId,ConsumedService\n".to_owned();
    for n in 0..24 {
        csv.push_str(&format!(
            "2026-10-01,0.1,USD,s1,{}-{n}\n",
            "x".repeat(5_000)
        ));
    }
    let (_server, shared, azure) = cloud(csv.into_bytes()).await;
    shared.lock().unwrap().fail_after = Some(1);
    assert!(runner::run(&azure, &config, day()).await.is_err());
    let saved = shared.lock().unwrap().state();
    let Phase::Writing { pending, .. } = saved.phase else {
        panic!("must retain a write checkpoint")
    };
    assert_eq!(pending.completed.len(), 1);
    assert!(pending.batch_count > 1);
    shared.lock().unwrap().fail_after = None;
    let result = runner::run(&azure, &config, day()).await.unwrap();
    assert_eq!(result["totals"]["input_rows"], 24);
    assert_eq!(result["totals"]["output_rows"], 24);
    let cloud = shared.lock().unwrap();
    assert_eq!(cloud.report_requests, 1);
    assert_eq!(cloud.bodies.len(), pending.batch_count);
    assert_eq!(cloud.ranges, vec![None, Some(pending.start.offset)]);
    let mut ids = std::collections::BTreeSet::new();
    for row in cloud
        .bodies
        .iter()
        .flat_map(|body| body.as_array().unwrap())
    {
        assert!(ids.insert(row["RecordId"].as_str().unwrap()));
    }
    assert_eq!(ids.len(), 24);
}

#[tokio::test]
async fn concurrent_acquisition_has_one_winner_and_fences_stale_owners() {
    let (_server, shared, azure) = cloud(Vec::new()).await;
    let config = config();
    let (first, second) = tokio::join!(
        Lease::acquire(&azure, &config),
        Lease::acquire(&azure, &config)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let lease = first.or(second).unwrap();
    {
        let mut cloud = shared.lock().unwrap();
        cloud.checkpoint.as_mut().unwrap()["Owner"] = json!("replacement-owner");
        cloud.etag += 1;
    }
    assert!(
        lease
            .apply(Event::ReportRequested("operation".into()))
            .await
            .is_err()
    );
    assert!(lease.ensure_owned().await.is_err());
    assert!(lease.finish().await.is_err());
    let cloud = shared.lock().unwrap();
    assert!(matches!(cloud.state().phase, Phase::Ready));
    assert_eq!(
        cloud.checkpoint.as_ref().unwrap()["Owner"],
        "replacement-owner"
    );
}

#[tokio::test]
async fn lease_release_and_configuration_fingerprinting_are_persistent() {
    let (_server, _cloud, azure) = cloud(Vec::new()).await;
    let mut config = config();
    Lease::acquire(&azure, &config)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    config.limits.concurrency = 2;
    config.job.lag_days = 5;
    config.azure.provision = false;
    Lease::acquire(&azure, &config)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    config.aggregation.dimensions.clear();
    assert!(Lease::acquire(&azure, &config).await.is_err());
    assert!(state::status(&azure, &config).await.unwrap()["checkpoint"].is_object());
}

#[tokio::test]
async fn retries_throttling_and_handles_empty_days_without_downloading() {
    let (_server, shared, azure) =
        cloud(include_bytes!("../tests/fixtures/costs.csv").to_vec()).await;
    shared.lock().unwrap().throttle = 2;
    runner::run(&azure, &config(), day()).await.unwrap();
    assert_eq!(shared.lock().unwrap().bodies.len(), 1);
    let (_server, shared, azure) = cloud(Vec::new()).await;
    shared.lock().unwrap().empty = true;
    let result = runner::run(&azure, &config(), day()).await.unwrap();
    assert_eq!(result["totals"]["completed_days"], 1);
    assert!(shared.lock().unwrap().ranges.is_empty());
}

#[tokio::test]
async fn event_hub_provisions_dcr_identity_receiver_role_and_association() {
    let (_server, shared, azure) =
        cloud(include_bytes!("../tests/fixtures/costs.csv").to_vec()).await;
    let mut config = config();
    config.target = Target::EventHub {
        namespace: "gotomoch-events".into(),
        hub: "costs".into(),
        consumer_group: "gotomoch".into(),
    };
    config.azure.workspace_capacity_gb = Some(100);
    let result = runner::run(&azure, &config, day()).await.unwrap();
    assert_eq!(result["delivery"], "event-hub");
    let cloud = shared.lock().unwrap();
    let events = cloud.bodies[0].as_array().unwrap();
    assert_eq!(events.len(), 3);
    let record: Value = serde_json::from_str(events[0]["Body"].as_str().unwrap()).unwrap();
    assert!(record["Dimensions"].is_object());
    let dcr_id = config.resource_id(
        "Microsoft.Insights",
        "dataCollectionRules",
        &config.azure.dcr,
    );
    assert_eq!(
        cloud.resources[&dcr_id]
            .pointer("/properties/dataSources/dataImports/eventHub/consumerGroup"),
        Some(&json!("gotomoch"))
    );
    assert!(
        cloud.resources[&dcr_id]["properties"]["dataFlows"][0]["transformKql"]
            .as_str()
            .unwrap()
            .contains("parse_json(RawData)")
    );
    assert!(cloud.resources.iter().any(
        |(id, value)| id.contains("/dataCollectionRuleAssociations/")
            && value["properties"]["dataCollectionRuleId"] == dcr_id
    ));
    assert!(
        cloud
            .resources
            .iter()
            .any(|(id, value)| id.contains("/roleAssignments/")
                && value["properties"]["principalType"] == "ServicePrincipal"
                && value["properties"]["principalId"] == "22222222-2222-2222-2222-222222222222")
    );
}

#[tokio::test]
async fn event_hub_cannot_silently_create_a_paid_commitment_workspace() {
    let (_server, shared, azure) =
        cloud(include_bytes!("../tests/fixtures/costs.csv").to_vec()).await;
    let mut config = config();
    config.target = Target::EventHub {
        namespace: "gotomoch-events".into(),
        hub: "costs".into(),
        consumer_group: "gotomoch".into(),
    };
    assert!(
        runner::run(&azure, &config, day())
            .await
            .unwrap_err()
            .to_string()
            .contains("preparing ingestion")
    );
    assert!(
        shared
            .lock()
            .unwrap()
            .resources
            .keys()
            .all(|id| !id.contains("/workspaces/"))
    );
}

#[tokio::test]
async fn range_resume_and_gzip_resume_produce_identical_rows() {
    for compressed in [false, true] {
        let config = config();
        let csv = include_bytes!("../tests/fixtures/costs.csv");
        let bytes = if compressed {
            let mut encoder = async_compression::tokio::bufread::GzipEncoder::new(&csv[..]);
            let mut bytes = Vec::new();
            encoder.read_to_end(&mut bytes).await.unwrap();
            bytes
        } else {
            csv.to_vec()
        };
        let (_server, shared, azure) = cloud(bytes.clone()).await;
        let blob = crate::azure::Blob {
            blob_link: "https://reports.blob.core.windows.net/report.csv".into(),
            byte_count: bytes.len() as u64,
        };
        let mut config = config;
        config.limits.chunk_rows = 2;
        let mut source = azure
            .open_blob(&blob, compressed, Cursor::default(), &config)
            .await
            .unwrap();
        let first = source.read_chunk(&config, None).await.unwrap();
        let next = source.read_chunk(&config, None).await.unwrap();
        let mut resumed = azure
            .open_blob(&blob, compressed, first.end.clone(), &config)
            .await
            .unwrap();
        let replay = resumed.read_chunk(&config, None).await.unwrap();
        assert_eq!(next.digest, replay.digest);
        assert_eq!(next.end, replay.end);
        assert_eq!(
            shared.lock().unwrap().ranges[1],
            if compressed {
                None
            } else {
                Some(first.end.offset)
            }
        );
    }
}

#[tokio::test]
async fn rejects_changed_etags_and_untrusted_poll_urls() {
    let (_server, _shared, azure) =
        cloud(include_bytes!("../tests/fixtures/costs.csv").to_vec()).await;
    let blob = crate::azure::Blob {
        blob_link: "https://reports.blob.core.windows.net/report.csv".into(),
        byte_count: 1,
    };
    assert!(
        azure
            .open_blob(
                &blob,
                false,
                Cursor {
                    etag: Some("\"old\"".into()),
                    ..Cursor::default()
                },
                &config()
            )
            .await
            .is_err()
    );
    assert!(
        azure
            .poll_report("https://attacker.example/operation")
            .await
            .is_err()
    );
    assert!(
        azure
            .request(
                Method::GET,
                "http://management.azure.com/",
                None,
                HeaderMap::new(),
                None
            )
            .await
            .is_err()
    );
}

#[test]
fn retry_after_uses_the_largest_service_limit_and_rejects_invalid_values() {
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", "2".parse().unwrap());
    headers.insert(
        "x-ms-ratelimit-microsoft.costmanagement-qpu-retry-after",
        "7".parse().unwrap(),
    );
    assert_eq!(retry_after(&headers).unwrap(), Some(Duration::from_secs(7)));
    headers.insert("retry-after", "invalid".parse().unwrap());
    assert!(retry_after(&headers).is_err());
}

#[tokio::test]
async fn fully_acknowledged_final_chunk_needs_no_source_or_destination() {
    let (_server, shared, azure) = cloud(Vec::new()).await;
    let config = config();
    let cursor = Cursor {
        offset: 50,
        headers: vec!["header".into()],
        etag: Some("\"blob1\"".into()),
        row: 0,
    };
    let state = State::new(day(), config.fingerprint().unwrap())
        .transition(Event::ReportRequested(format!(
            "https://management.azure.com{OPERATION}"
        )))
        .unwrap()
        .transition(Event::ReportReady(1))
        .unwrap()
        .transition(Event::SourceOpened(cursor.clone()))
        .unwrap()
        .transition(Event::ChunkPrepared(Pending {
            start: cursor.clone(),
            end: Cursor {
                offset: 100,
                row: 2,
                ..cursor
            },
            input_rows: 2,
            eof: true,
            digest: "0".repeat(64),
            observed_at: Utc::now(),
            batch_count: 1,
            completed: Default::default(),
        }))
        .unwrap()
        .transition(Event::BatchAccepted {
            index: 0,
            records: 1,
        })
        .unwrap();
    {
        let mut cloud = shared.lock().unwrap();
        cloud.checkpoint = Some(json!({
            "PartitionKey": "jobs", "RowKey": config.job.id, "Owner": "",
            "LeaseUntil": Utc::now().to_rfc3339(), "State": serde_json::to_string(&state).unwrap(),
        }));
        cloud.etag = 1;
    }
    let result = runner::run(&azure, &config, day()).await.unwrap();
    assert_eq!(result["totals"]["input_rows"], 2);
    assert_eq!(result["totals"]["output_rows"], 1);
    let cloud = shared.lock().unwrap();
    assert_eq!(cloud.report_requests, 0);
    assert!(cloud.ranges.is_empty());
    assert!(cloud.bodies.is_empty());
    assert!(!cloud.requests.iter().any(|(_, path)| path == OPERATION));
    assert!(
        !cloud
            .resources
            .keys()
            .any(|path| path.contains("/workspaces/"))
    );
}

#[tokio::test]
async fn keeps_one_stream_open_across_several_committed_chunks() {
    let (_server, shared, azure) =
        cloud(include_bytes!("../tests/fixtures/costs.csv").to_vec()).await;
    let mut config = config();
    config.limits.chunk_rows = 2;
    let result = runner::run(&azure, &config, day()).await.unwrap();
    assert_eq!(result["totals"]["input_rows"], 5);
    assert_eq!(result["totals"]["output_rows"], 4);
    let cloud = shared.lock().unwrap();
    assert_eq!(cloud.bodies.len(), 3);
    assert_eq!(cloud.ranges, vec![None]);
}

#[tokio::test]
async fn expired_lease_is_taken_over_conditionally() {
    let (_server, shared, azure) = cloud(Vec::new()).await;
    let config = config();
    let original = Lease::acquire(&azure, &config).await.unwrap();
    let previous = shared.lock().unwrap().checkpoint.as_ref().unwrap()["Owner"].clone();
    drop(original);
    {
        let mut cloud = shared.lock().unwrap();
        cloud.checkpoint.as_mut().unwrap()["LeaseUntil"] =
            json!((Utc::now() - chrono::Duration::seconds(1)).to_rfc3339());
        cloud.etag += 1;
    }
    let replacement = Lease::acquire(&azure, &config).await.unwrap();
    assert_ne!(
        shared.lock().unwrap().checkpoint.as_ref().unwrap()["Owner"],
        previous
    );
    replacement.finish().await.unwrap();
}

#[tokio::test]
async fn reuses_server_enriched_resources_but_rejects_behavioral_drift() {
    let (_server, shared, azure) = cloud(Vec::new()).await;
    let config = config();
    azure.ensure_state_resources(&config).await.unwrap();
    azure.ensure_target(&config).await.unwrap();
    azure.ensure_target(&config).await.unwrap();
    let dcr = config.resource_id(
        "Microsoft.Insights",
        "dataCollectionRules",
        &config.azure.dcr,
    );
    {
        let mut cloud = shared.lock().unwrap();
        cloud.resources.get_mut(&dcr).unwrap()["properties"]["dataFlows"][0]["builtInTransform"] =
            json!("UnexpectedTransform");
    }
    assert!(azure.ensure_target(&config).await.is_err());
    {
        let mut cloud = shared.lock().unwrap();
        cloud.resources.get_mut(&dcr).unwrap()["properties"]["dataFlows"][0]["builtInTransform"] =
            Value::Null;
        cloud.resources.get_mut(&dcr).unwrap()["properties"]["dataFlows"][0]["outputStream"] =
            json!("Custom-Wrong_CL");
    }
    assert!(azure.ensure_target(&config).await.is_err());
}
