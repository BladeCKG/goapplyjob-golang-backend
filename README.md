# An example Gin Go project

## Description

A boilerplate project for a Gin webserver.
Idea is to follow best practices and have a good starting point for any
project which involves one of the following or all of them:

* **PostgreSQL** database access with **sqlc**
* Local PostgreSQL integration tests
* **Gin Gonic** Web server
* Additional yet useful static code analysis with **golangci-lint**
* Have a nice structured logging with tracing offered by **Zerolog**
* Enable **OpenTelemetry** tracing for better view on performance insights

Example logging with Zerolog middleware hooked to Gin Gonic:

```json
{"level":"info","uniq_id":"a05830c0","time":"2022-12-18T23:03:26+02:00","caller":"example-gin/cmd/webserver/main.go:38","message":"test"}
{"level":"info","client_ip":"127.0.0.1","uniq_id":"a05830c0","method":"GET","status_code":200,"body_size":23,"path":"/get/asdf","latency":"6.185µs","time":"2022-12-18T23:03:33+02:00"}
```

Example screenshot of OTEL tracing captured and shown in Jeager UI:
![OTEL Tracing](otel_tracing.png)

## Prerequisites

* [Docker](https://docker-docs.netlify.app/install/) acting as a local development database and for integration tests.
  Also a step towards reproducible builds.
* [Gin](https://gin-gonic.com/) as a web framework.
* [Golangci-lint](https://golangci-lint.run/) for linting and static code analysis.
* [govulncheck](https://go.dev/security/vuln/) for package vulnerability analysis.
* [Goose](https://github.com/pressly/goose) for database migrations.
* [sqlc](https://sqlc.dev/) for database access.
* [Zerolog](https://zerolog.io/) for structured and leveled logging.

## Features

* Logging has an unique ID `uniq_id` column that can be used to track
  log events in clustered application and other places where same service
  has many instances.
* Graceful shutdown for HTTP server and for the database connections.
  * Useful with Lambdas or with AWS Fargate where signal handling speeds up
    the shutdown.
* Database retry with exponential backoff and connection pooling.
* Gin middlewares for logging and database.
* Enable OpenTelemetry tracing with Gin Gonic integration by default.
* Live performance statistics and profiling via separate web service (pprof HTTP server)
* No logging for certain routes/paths (currently /health and /metrics).

## Usage

### Install package dependencies

Type `make install-dependencies` to retrieve Go packages needed by the project.

### Configuration

Open telemetry sampling rate can be configured by setting the `OTEL_SAMPLING_RATE`
environmental variable from 0.0 to 1.0.
0.0 means no tracing and 1.0 means include all traces.
Value is measured in percents.
Therefore 0.5 means 50% of the samples will be collected.

### Demoing usage

```bash
make stop-db start-db build-webserver
export $(grep -v "^#" .env |xargs)
./target/webserver_linux_amd64 &
./t.sh
```

### One-Time Skippable Recheck Worker

Re-checks `raw_us_jobs` rows marked `is_skippable=true`. If the normalized URL no longer returns `404`, the worker
marks the row as `is_skippable=false` and `is_ready=false` so it can be processed again.

Run:

```bash
go run ./cmd/skippablerecheck
```

Optional env:

```env
SKIPPABLE_RECHECK_BATCH_SIZE=100
```

### Fresh Environment Bootstrap (Linux + Windows)

Start the current backend commands in one step:

Linux:

```bash
cd <backend-directory>
chmod +x scripts/bootstrap_and_start.sh
./scripts/bootstrap_and_start.sh
```

Windows PowerShell:

```powershell
cd <backend-directory>
.\scripts\bootstrap_and_start.ps1
```

What these scripts do:

* create `.env` from `.env.example` if missing
* ensure `logs/` exists
* run `go run ./cmd/migrate`
* start the Go API and watcher commands in background

### Docker Bootstrap (Linux + Windows)

Linux:

```bash
cd <backend-directory>
chmod +x scripts/docker_bootstrap_and_start.sh
./scripts/docker_bootstrap_and_start.sh
```

Windows PowerShell:

```powershell
cd <backend-directory>
.\scripts\docker_bootstrap_and_start.ps1
```

Run migrations before API or workers:

```bash
go run ./cmd/migrate
```

DB health endpoint: `http://127.0.0.1:8080/db/health`.

### Render Deployment (API + Workers)

This repo includes `render.yaml` for one-click Render Blueprint deployment with:

* 1 PostgreSQL database
* 1 web service for the Gin API
* background workers for:
  * `watcher`
  * `importer`

Steps:

1. Push this backend folder to GitHub.
2. In Render, choose **New +** -> **Blueprint** and select the repository.
3. Render reads `render.yaml` and creates the database and services.
4. Set all `sync: false` env vars in Render before the first production run.

Required manual env vars:

* `WATCH_URL`
* `SMTP_HOST`
* `SMTP_USER`
* `SMTP_PASS`
* `SMTP_FROM`
* `AUTH_MAGIC_LINK_BASE_URL`
* `PAYMENT_SUCCESS_URL`
* `PAYMENT_CANCEL_URL`
* `CRYPTO_IPN_CALLBACK_URL`
* `OXAPAY_MERCHANT_API_KEY`

Notes:

* `DATABASE_URL` is wired from Render Postgres.
* `AUTH_COOKIE_SECURE=true` is already set in `render.yaml`.

### GitHub Actions Worker Runner

This repo includes `.github/workflows/workers-cron.yml` for scheduled worker execution in GitHub Actions.

Schedule:

* every hour: `watcher` and `importer`
* every 3 hours: `parsedfreshness`
* manual `workflow_dispatch`: runs all configured worker steps immediately

Required repository secrets:

* `DATABASE_URL`
* `WATCH_URL`

### Adding new database migrations

This expects `goose` to be installed and it can be found from the `$PATH`:

```bash
make name=create-shop migrate-add
```

The newly added migration can be found under [sql/schemas/](sql/schemas/).

## TODO

* Create separate unprivileged API users for Postgres access
* Add an example of proper database transaction cancellation with Golang's cancel
