# gotomoch

A Rust CLI that incrementally reads Azure Cost Management cost details, aggregates
them, and sends compact, typed records to Log Analytics.

```text
Cost Details API -> streaming CSV -> decimal aggregation -> Logs Ingestion API -> DCR -> LAW
                                                       or
                                                       -> Event Hub -> DCR -> LAW

Azure Table Storage: one ETag-protected lease + checkpoint entity per job
```

The implementation follows Ousterhout's deep-module approach and *Grokking
Simplicity*'s functional-core/imperative-shell approach: parsing, aggregation,
encoding, and state transitions are calculations; Azure access and orchestration
are actions. There is no workflow framework or custom authentication chain.

## Quick start

Install a current stable Rust toolchain and Azure CLI. Linux builds also need
`pkg-config` and OpenSSL development headers for the Azure SDK. Then:

```sh
cargo build --release --locked
cp examples/direct.toml gotomoch.toml
# Edit the subscription, scope, start date, names, and region in gotomoch.toml.
az login
./target/release/gotomoch check
./target/release/gotomoch run
./target/release/gotomoch status
```

`run` processes through **UTC today minus `job.lag_days`**, inclusively; the
default lag is three days. Override the upper bound with
`gotomoch run --until 2026-10-10`. An explicit upper bound must be before today.
Schedule the same command daily; once caught up, it does not request another cost
report, download source data, or initialize the destination.

To inspect the transformation without credentials, network access, or Azure writes:

```sh
cargo run --locked -- preview --config examples/direct.toml --input tests/fixtures/costs.csv
```

`preview` accepts CSV and gzip-compressed CSV files and writes JSON Lines.
`run` writes a JSON summary with cumulative acknowledged counts. Diagnostic logs
go to stderr; set `RUST_LOG=gotomoch=debug` to change verbosity. All commands return
a nonzero exit code on failure.

## Authentication and permissions

Authentication uses the Azure SDK's **`DefaultAzureCredential`**, shared and cached
across management, storage, Monitor, and Event Hubs requests. Its supported chain
is environment credentials, App Service/VM managed identity, then Azure CLI.
For a service principal, provide `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and
`AZURE_CLIENT_SECRET` through your environment or secret manager, not configuration.
Federated workload identity is supported through the environment credential with
`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_FEDERATED_TOKEN_FILE` (or
`AZURE_FEDERATED_TOKEN`). The managed-identity steps in this SDK's default chain
use system-assigned identities; `AZURE_CLIENT_ID` alone does not select a
user-assigned managed identity.

The project uses Azure Identity 0.21, which exposes the SDK's
`DefaultAzureCredential`. Later Rust SDK lines removed this type; upgrading
requires preserving the authentication contract.

For initial provisioning, the running identity needs resource creation permissions
and role-assignment permissions, such as Contributor plus Role Based Access
Control Administrator at the deployment resource group. Creating a missing
resource group and registering missing resource providers additionally need the
appropriate subscription-level permissions. Source cost access is separate:
grant Cost Management Reader at the source subscription, or the corresponding
EA/MCA billing-scope permission.

With `azure.provision = true`, the tool creates missing resources, registers
required resource providers, and grants these narrowly scoped data roles to the
current identity:

| Role | Scope |
| --- | --- |
| Storage Table Data Contributor | Checkpoint storage account |
| Monitoring Metrics Publisher | DCR, for direct delivery |
| Azure Event Hubs Data Sender | Event hub, for buffered delivery |
| Azure Event Hubs Data Receiver | Event hub, assigned to the DCR's managed identity |

After bootstrap, set `azure.provision = false` to prohibit resource and RBAC
creation. The runtime identity still needs the applicable data roles, cost access,
and ARM read access to the configured resources. RBAC propagation is retried for
up to three minutes at initial storage access and ingestion startup; slower
propagation fails explicitly and can be retried safely.

`azure.principal_id`, when supplied, must be the **object ID of the running
identity**, not its application/client ID. Otherwise it is read from the ARM
token's `oid` claim.

## Resources created on demand

Direct mode uses a resource group, Standard LRS StorageV2 account,
`GotomochState` storage table, Log Analytics workspace, custom log table, DCE,
DCR with managed identity, and the required role assignments.
The DCE name is `<dcr>-endpoint`. The default log table is `AzureCosts_CL`.
The DCR and DCE must be in the configured workspace region.

Existing resources are reused, not overwritten. A conflicting table schema,
DCR flow, destination, region, or association is an error. Use a dedicated DCR
and Event Hub consumer group for the pipeline. Names for storage accounts and
Event Hubs namespaces must be globally unique. New storage accounts disable
shared-key access and public blob access; data-plane calls use Entra tokens.

Creating resources can incur Azure charges. No resources are deleted by the CLI.
Resource creation is idempotent, but it is not a transactional deployment: if
bootstrap fails, resources already created remain for the next attempt.

### Event Hub buffering

Use `examples/event-hub.toml` or change the target section:

```toml
[target]
mode = "event-hub"
namespace = "your-globally-unique-namespace"
hub = "costs"
consumer_group = "gotomoch"
```

The tool also creates a Standard Event Hubs namespace, a hub with seven-day
retention, a consumer group, a DCR data-import source, its receiver role, and a
**data collection rule association on the event hub**. Events carry individual
JSON records. The DCR parses `RawData` into the same typed schema as direct mode.
No custom relay service is required.

**Azure's native Event Hub-to-DCR ingestion is public preview.** It requires a
workspace on a commitment tier or linked to a dedicated cluster, an available
supported region, and Event Hubs network access for the Azure service. Existing
pay-as-you-go workspaces are not silently upgraded. To create a new commitment
workspace, explicitly set `workspace_capacity_gb = 100` or another supported
capacity in `[azure]`. This is a **paid daily commitment**, not a spending cap.
Check pricing before enabling it.

A successful Event Hub batch means Azure accepted the events into the broker,
not that the DCR delivered them to LAW. Monitor the DCR, consumer lag, and retention;
a stalled downstream consumer can lose events after retention expires.
Regional service capacity can prevent preview ingestion from starting even
after ARM configuration succeeds.

## Aggregation and typed output

The default groups by **cost date, billing currency, SubscriptionId, and
ConsumedService**, sums cost, and counts source rows. Date and currency are
always part of the key; different currencies are never added together.
To reduce data further, choose fewer dimensions:

```toml
[aggregation]
dimensions = [] # Cost totals by date and currency only.
# Optional explicit input-column overrides:
# cost_column = "CostInBillingCurrency"
# date_column = "Date"
# currency_column = "BillingCurrencyCode"
```

Column lookup ignores case and punctuation. Automatic mappings cover common
Azure native cost exports and FOCUS: `Date`/`UsageDate`/`UsageDateTime`/
`ChargePeriodStart`, `CostInBillingCurrency`/`PreTaxCost`/`Cost`, and billing-currency
columns. FOCUS uses `BilledCost` for ActualCost and `EffectiveCost` for
AmortizedCost. SubscriptionId also recognizes SubscriptionGuid/SubAccountId;
ConsumedService also recognizes ServiceName. Other dimensions select matching
input columns. Missing, duplicate, or ambiguous headers and invalid values fail
explicitly; rows are not silently dropped.

Costs are accumulated using checked, base-10 decimal arithmetic, including
credits and scientific notation. Serialization preserves a JSON numeric value;
LAW ultimately stores it as a `real`, with that type's floating-point precision.
Dates, counts, and dimensions become datetime, long, and dynamic fields rather
than retaining a wide string-only copy of every source row.

| Output field | LAW type | Meaning |
| --- | --- | --- |
| TimeGenerated | datetime | Stable preparation time of the checkpointed chunk |
| CostDate | datetime | Original charge/usage date, at midnight |
| Currency | string | Billing currency |
| Metric | string | ActualCost or AmortizedCost |
| Dimensions | dynamic | Selected grouping columns |
| Cost | real | Summed billing-currency cost |
| Rows | long | Number of represented source rows |
| RecordId | string | Deterministic identity for this chunk and group |
| JobId | string | Logical migration ID |

Aggregation is **per bounded source chunk**, not across an entire unbounded
report. Groups can occur in several chunks; sum their output rows in queries.
Unselected dimensions and raw source columns are intentionally discarded.
The defaults are 5,000 input rows or roughly 8 MB of field data per chunk,
900,000 encoded bytes per request, and four concurrent requests.
Actual serialized transport size, including Event Hub envelopes and escaping,
is measured before sending. Records above 60,000 encoded JSON bytes are rejected
to avoid Azure field/message truncation.

Memory depends on a bounded chunk, its aggregate groups, encoded batches, and
the CSV parser's largest record, not total report size. Records over 1 MB of field
data are rejected after parsing; that is not a hard allocation cap on malicious
CSV input. The configured source is Azure's generated cost report, not an
untrusted CSV upload service.

## Incremental state and concurrency

The durable finite state machine is:

```text
Ready -> AwaitingReport -> Reading -> Writing -> Reading
  ^                          |                    |
  +--------- next day <------+------ end of day --+
```

Each job has one special entity in `GotomochState`:
`PartitionKey = jobs`, `RowKey = job.id`. It holds both the owner/lease expiry and
the complete checkpoint. Inserting the entity or replacing it with `If-Match`
implements atomic acquisition. The two-minute lease is renewed every 30 seconds;
renewal failure cancels migration. Every state update uses the latest ETag.
No wildcard updates or process-local-only locks are used.

The checkpoint records the next unread day, report operation URL, blob index/count,
CSV header, byte/row cursor, source ETag, deterministic pending-chunk digest,
preparation timestamp, and **each acknowledged batch index**, including
out-of-order acknowledgements. A read cursor advances only after all output
batches for that chunk are acknowledged.

Uncompressed CSV resumes with HTTP Range and If-Match rather than downloading
committed prefixes. A live run keeps the stream open across chunks. Gzip is also
streamed; after a restart it must replay the compressed prefix to the saved
uncompressed offset because arbitrary gzip offsets are not seekable.
An unfinished chunk is recomputed deterministically and already-acknowledged
batches are skipped.

An entirely acknowledged final chunk can be committed after a restart without
reopening its source or destination. Destinations are initialized lazily only
when a batch actually needs to be sent. Lease participants must have synchronized
UTC clocks; the process stops work before its local lease deadline.

A fingerprint prevents changing the source, grouping, chunk boundaries,
encoding limit, or destination under an existing checkpoint. Concurrency,
settlement lag, and provisioning permissions can change without invalidating it.
Use a distinct job ID when intentionally starting a different migration.
All instances of the **same logical job must use the same storage account and
job ID**; unrelated job IDs are not mutually exclusive.

The lease fences checkpoint writes, but neither LAW nor Event Hubs accepts that
fencing token. A request already in flight when ownership is lost can complete.
Likewise, a process can fail after the sink accepts data but before recording
the acknowledgement. Delivery is therefore **at least once**, not exactly once.
Query by RecordId to remove retry duplicates before adding costs:

```kusto
AzureCosts_CL
| where JobId == "subscription-costs"
| summarize arg_max(TimeGenerated, *) by RecordId
| summarize Cost = sum(Cost), SourceRows = sum(Rows)
    by CostDate, Currency, Metric, Service = tostring(Dimensions.ConsumedService)
```

Distinct identical source rows still contribute to `Rows` and `Cost` during
aggregation. The identity includes the job, blob, chunk position, input digest,
and group; it does not collapse legitimate repeated charges.

## Source and operational limits

This implementation targets **Azure public cloud** and the Cost Details API
(`2024-08-01`), which is intended for supported EA/MCA scopes. Azure can restrict
other agreement types. Each report requests a single closed day; the service
allows at most one month per report and approximately 13 months of history.
This CLI does not backfill older storage exports through `run`; local exports
can be inspected with `preview`.

Completed days are treated as immutable. The default settlement lag reduces
exposure to late-arriving charges but does **not** guarantee a finalized invoice.
Late adjustments to completed days are not automatically reread or upserted:
LAW is append-only. To reconcile a corrected interval, use a new job ID starting
at that interval and query the selected revision separately rather than adding
both jobs' snapshots. Do not manually rewind a live checkpoint.

Cost-report download links and operations expire. If a partly acknowledged
report has expired, changed ETag/digest, or disappeared, the tool stops rather
than silently regenerating a different report and skipping rows. Preserve the
checkpoint, inspect `status`, and reconcile that day explicitly with a new job
if the original report cannot be recovered.

Transient request failures and 408/429/500/502/503/504 responses are retried
with backoff and service-provided Retry-After headers. Other failures are
surfaced with Azure status, error code, and request ID. Report polling has a
one-hour per-run timeout; its operation is retained for a subsequent run.
A mid-stream CSV/download failure stops at the last durable checkpoint; invoke
`run` again to resume.
Bearer tokens are never forwarded to signed report blobs, redirects are disabled,
and SAS links/tokens are neither logged nor stored in checkpoints.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```

The mock-Azure suite exercises full resource bootstrap, both delivery protocols,
throttling, checkpoint reuse, partial-write recovery, byte-range/gzip resumption,
concurrent acquisition, stale-owner fencing, and cost/schema validation. Live
Azure behavior, regional preview capacity, and tenant permissions must also be
verified in the deployment subscription before production use.

Official contracts:
[Cost Details](https://learn.microsoft.com/en-us/rest/api/cost-management/generate-cost-details-report/create-operation),
[Logs Ingestion](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview),
[Event Hub ingestion preview](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/ingest-logs-event-hub).
