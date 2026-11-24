# CosmosDB Diagnostic Exporter

Polls Azure Blob Storage for CosmosDB diagnostic logs and exports metrics via OTLP. Each minute's blob is aggregated into gauge metrics with the timestamp set to `:00` of that minute. The latest minute is always skipped (it may still be receiving writes).

## How it works

1. Lists diagnostic log blobs across all Azure containers (`DataPlaneRequests`, `PartitionKeyRUConsumption`, `QueryRuntimeStatistics`)
2. Groups blobs by minute, excludes the latest minute
3. Builds a partition mapping from `PartitionKeyRUConsumption` data
4. Enriches `DataPlaneRequests` with `partition_key_range_id` using the mapping
5. Aggregates records into per-minute OTLP metrics and exports them
6. Tracks the top 5 slowest requests, top 5 costliest requests (per collection/operation), and top 5 costliest queries — logs them hourly
7. Saves progress to a checkpoint file to resume on restart

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `-storage-account-url` | (required) | Azure Blob Storage account URL |
| `-storage-account-key` | `""` | Azure storage account key. If set, Shared Key authentication is used (env: `AZURE_STORAGE_KEY`) |
| `-subscription-id` | `""` | Azure subscription ID. Only used together with `-storage-resource-group` to look up a key via ARM (see Authentication) |
| `-storage-resource-group` | `""` | Resource group of the storage account. Only used together with `-subscription-id` |
| `-blob-path-prefix` | `""` | Prefix to scope blob listing (e.g. `resourceId=SUBSCRIPTIONS/...`) |
| `-poll-interval` | `1m` | How frequently to poll for new blobs |
| `-checkpoint-file` | `./checkpoint.json` | Path to the checkpoint file |
| `-max-age` | `90m` | Maximum age of blobs to process |
| `-otlp-endpoint` | `""` | OTLP HTTP endpoint URL. Falls back to `OTEL_EXPORTER_OTLP_ENDPOINT` env var |
| `-otlp-username` | `""` | OTLP basic auth username (env: `OTLP_USERNAME`) |
| `-otlp-password` | `""` | OTLP basic auth password (env: `OTLP_PASSWORD`) |

## Authentication

The exporter picks the Blob Storage authentication mode at startup and logs it (`selected blob storage authentication mode`); keys are never logged.

1. **Shared Key**: `-storage-account-key` (or `AZURE_STORAGE_KEY`) is set.
2. **Microsoft Entra ID** (default when no key is set): uses [`DefaultAzureCredential`](https://learn.microsoft.com/azure/developer/go/sdk/authentication/credential-chains#defaultazurecredential-overview) to get a token, so it works on storage accounts with shared key access disabled. On AKS, use [Azure workload identity](https://learn.microsoft.com/azure/aks/workload-identity-overview): label the pod `azure.workload.identity/use: "true"` and annotate its service account with `azure.workload.identity/client-id`; the webhook injects `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_FEDERATED_TOKEN_FILE`. The identity needs the **Storage Blob Data Reader** role on the storage account (or the container scope).
3. **ARM key lookup** (legacy): no key, but both `-subscription-id` and `-storage-resource-group` are set. The exporter fetches an account key via the ARM `ListKeys` API and uses Shared Key. This requires shared key access to be enabled and key-listing permission. Omit both flags to use Entra ID instead.

Setting only one of `-subscription-id` / `-storage-resource-group` is an error.

## Metrics

All metrics include the labels `cluster`, `account_name`, `database`, `collection`, `operation`, `status_code`, and `partition_key_range_id`.

| Metric | Type | Extra Labels | Description |
|--------|------|--------------|-------------|
| `cosmosdb_data_plane_requests_1m` | Gauge | | Request count per minute |
| `cosmosdb_data_plane_request_duration_seconds` | Gauge | `quantile` | Duration quantiles (`avg`, `0.5`, `0.75`, `0.95`, `0.99`, `1`) |
| `cosmosdb_data_plane_request_charge_ru_1m` | Gauge | | RU consumed per minute |

## Hourly top-N logging

Every hour, the exporter logs the top 5 slowest and top 5 most expensive (by RU) individual requests per `(collection, operation)` pair, plus the top 5 most expensive SQL queries globally. These are collected incrementally during normal processing (no extra cost) and emitted as logfmt lines.

**Request log fields**: `collection`, `operation`, `rank`, `activity_id`, `status_code`, `duration_sec`, `ru_charge`, `partition_key_range_id`, `document`

**Query log fields**: `collection`, `rank`, `activity_id`, `duration_sec`, `ru_charge`, `partition_key_range_id`, `rows_returned`, `query`

Query tracking joins `QueryRuntimeStatistics` records to `DataPlaneRequests` via `activityId` to obtain RU and duration.

## Partition mapping

The exporter resolves `partition_key_range_id` for DataPlaneRequests by joining with PartitionKeyRUConsumption data:

- **Exact match**: the document ID extracted from `requestResourceId` matches a partition key directly (e.g. chunks collection)
- **Numeric suffix match**: for numeric document IDs, `doc_id / 100000` is matched against the numeric suffix of partition keys (e.g. logs collection)

When the mapping cannot be resolved, `partition_key_range_id` is empty.

## Running locally

```sh
make run
```

## Building

```sh
make build-binary           # Linux amd64 + arm64
make build-and-push-image   # Multi-arch Docker image
```

## Deploying to Kubernetes

See [`deploy/kubernetes/sts.yaml`](deploy/kubernetes/sts.yaml) for an example StatefulSet. The StatefulSet uses a PVC to persist the checkpoint file across restarts.
