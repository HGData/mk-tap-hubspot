# `tap-hubspot`

tap-hubspot is a Singer tap for Hubspot.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| access_token        | False    | None    | Token to authenticate against the API service |
| client_id           | False    | None    | The OAuth app client ID. |
| client_secret       | False    | None    | The OAuth app client secret. |
| refresh_token       | False    | None    | The OAuth app refresh token. |
| start_date          | False    | None    | Earliest record date to sync |
| end_date            | False    | None    | Latest record date to sync |
| limit_events_month  | False    | None    | Hard limit the start date to last X months from today (no limit if not set) |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |
| batch_config        | False    | None    |             |

A full list of supported settings and capabilities is available by running: `tap-hubspot --about`

## Date Limiting

The `limit_events_month` parameter provides a way to hard limit how far back the tap will sync data, regardless of the configured `start_date`. This is useful for:

- Preventing excessive API usage when syncing large historical datasets
- Implementing rolling data windows for incremental syncs
- Controlling resource usage by limiting data volume

### How it works:

- When `limit_events_month` is set to a positive integer (e.g., `6`), the tap calculates a limit date by going back `31 * N` days from today
- The effective start date becomes the more recent of:
  - The configured `start_date` OR existing incremental state
  - The calculated limit date (31 * N days ago from today)
- This applies to both **initial runs** and **incremental syncs with existing state**
- If `limit_events_month` is not set or is `null`, the original `start_date` or incremental state is used without any limitation
- When limiting is applied, a log message will show the date change

### Examples:

**Initial Run:**
```yaml
# Configuration
start_date: "2020-01-01T00:00:00Z"
limit_events_month: 6

# Result: If today is 2025-07-18, the effective start date becomes 2024-01-20T00:00:00Z
# (186 days ago = 31 * 6), ignoring the older start_date of 2020-01-01
```

**Incremental Run with Old State:**
```yaml
# Configuration (incremental state exists from 2020-01-01)
limit_events_month: 12

# Log: "Limiting incremental state from 2020-01-01T00:00:00Z to 2023-07-21T00:00:00Z due to limit_events_month=12"
# Result: Uses 2023-07-21 instead of the old 2020-01-01 state
```

## Environment Variables

### `HG_SIMULATOR_TAP_HUBSPOT_BASE_URL` (dev only)

Optional override for the HubSpot API host. When set, every request the tap makes — both data calls and the OAuth token endpoint (`/oauth/v1/token`) — is sent to this host instead of `https://api.hubapi.com`. It is read directly from the process environment in `tap_hubspot/client.py`, where it conditionally overrides the `BASE_HUBSPOT_API_URL` constant (which otherwise stays at its `https://api.hubapi.com` default); it is not a `config.json` setting. A trailing slash, if present, is stripped.

This variable is injected externally — it is not normally set by the tap or its callers. In MK's data ingestion pipeline it is supplied by `mk-airflow` (`dags/data_ingestion_core.py:_apply_simulator_overrides`), which reads the `mdi_simulator_overrides` MWAA Airflow Variable and writes the resulting env vars into the pull task's ECS `containerOverrides`. That injection is gated behind `env == "dev"`, so the variable is never set on production pull tasks. When unset (the default everywhere outside the dev MWAA), the tap behaves identically to upstream and hits the real HubSpot API.

Intended use is to redirect a tenant's pull task at a hosted HubSpot simulator (`rgip-connector-simulators`, e.g. `https://hubspot-sim-01.hip.staging.hginsights.com`) for integration testing without touching production credentials.

## Elastic License 2.0

The licensor grants you a non-exclusive, royalty-free, worldwide, non-sublicensable, non-transferable license to use, copy, distribute, make available, and prepare derivative works of the software.

## Installation

```bash
pipx install git+https://github.com/ryan-miranda-partners/tap-hubspot.git
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

A Hubspot access token is required to make API requests. (See [Hubspot API](https://developers.hubspot.com/docs/api/working-with-oauth) docs for more info)


### Permissions

The following scopes need to be added to your access token to access the following endpoints:

- Contacts: `crm.schemas.contacts.read` or `crm.objects.contacts.read`
- Users: `settings.users.read`
- Ticket Pipeline: `media_bridge.read` or `crm.schemas.custom.read` or `timeline` or `tickets` or `e-commerce` or `crm.objects.goals.read`
- Deal Pipeline: `media_bridge.read` or `crm.schemas.custom.read` or `timeline` or `tickets` or `e-commerce` or `crm.objects.goals.read`
- Properties: All of `Tickets`, `crm.objects.deals.read`, `sales-email-read`, `crm.objects.contacts.read`, `crm.objects.companies.read`, `e-commerce`, `crm.objects.quotes.read`
- Owners: `crm.objects.owners.read`
- Companies: `crm.objects.companies.read`
- Deals: `crm.objects.deals.read`
- Feedback Submissions: `crm.objects.contacts.read`
- Line Items: `e-commerce`
- Products: `e-commerce`
- Tickets: `tickets`
- Quotes: `crm.objects.quotes.read` or `crm.schemas.quotes.read`
- Goals: `crm.objects.goals.read`
- Emails: `sales-email-read`

For more info on the streams and permissions, check the [Hubspot API Documentation](https://developers.hubspot.com/docs/api/overview).

## Usage

You can easily run `tap-hubspot` by itself or in a pipeline using [Meltano](https://meltano.com/).


### Streams Using v1 Endpoints

The following Streams use the v1 (legacy) endpoint in the Hubspot API:

1. [TicketPipeline & DealPipeline](https://legacydocs.hubspot.com/docs/methods/pipelines/pipelines_overview): The v3 endpoint requires a pipeline ID parameter to make calls to the API. Because of this,
you are limited to only pulling data for a single pipeline ID from v3, whereas the v1 API allows you to pull from all pipelines.
2. [EmailSubscriptions](https://legacydocs.hubspot.com/docs/methods/email/email_subscriptions_overview): The v3 endpoint requires you to set a single email address to pull subscription data, whereas
the v1 endpoint allows you to pull data from all emails.


## Stream Inheritance

This project uses parent-child streams. Learn more about them [here](https://gitlab.com/meltano/sdk/-/blob/main/docs/parent_streams.md).

### Executing the Tap Directly

```bash
tap-hubspot --version
tap-hubspot --help
tap-hubspot --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-hubspot` CLI interface directly using `poetry run`:

```bash
poetry run tap-hubspot --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-hubspot
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-hubspot --version
# OR run a test `elt` pipeline:
meltano elt tap-hubspot target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
