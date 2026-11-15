# cloudProject02

MSCS 621 Cloud Computing, Fall 2026, Marist University

## Project overview

This project is a Go web application for requesting weather forecasts by US ZIP code and number of days. It retrieves one to five days of data from OpenWeather, sends the forecast through Kafka, and evaluates temperature and wind alerts. Prometheus collects time-series data and Grafana plots it. The website displays Fahrenheit, miles per hour, and Eastern time.

## Use the hosted app

Open the [Oracle-hosted app](https://138.2.218.79/) in a browser. This option requires no installation, build, or SSH tunnel.

1. Sign in to [hosted Grafana](https://138.2.218.79/grafana/login) using the grading credentials supplied separately.
2. In the same browser, return to the weather app. Enter zip code, select days, and click **Get forecast**.
3. Review the embedded temperature and wind charts. Expand a date under **Forecast details** to see its forecast times, values, and alerts.

## Design/Architecture

- **Go web application.** The server validates the ZIP code and number of days, assigns a unique request ID, and displays a status page that refreshes while the request runs. Go templates render the form, alerts, and expandable daily tables. A JSON interface provides `POST /forecasts` for submission and `GET /forecasts/{id}` for results.
- **OpenWeather.** The app converts the ZIP code to coordinates, then requests OpenWeather's five-day forecast in metric units. It keeps the readings within the requested number of 24-hour periods. The provider supplies readings every three hours, and the website groups the available readings by Eastern calendar date.
- **Concurrency.** Four worker goroutines take jobs from a buffered Go channel that holds up to 16 waiting requests by default. A shared limiter spaces calls to OpenWeather by about 1.1 seconds. This lets different requests progress together while controlling pressure on the upstream API. Request IDs keep results separate. The app keeps up to 256 requests in memory, replacing the oldest finished result when full. An app restart clears the in-memory results.
- **Kafka and alerts.** A worker publishes the retrieved forecast to `weather.forecasts`, a topic with three partitions on one broker. A consumer in the same Go application evaluates the forecast and updates the result before committing the offset. An already completed request does not generate duplicate alerts. Alert limits are **100°F or higher**, **20°F or lower**, and **40 mph or higher**. The evaluator compares metric values and the presentation layer converts them for display.
- **Prometheus and Grafana.** Prometheus scrapes the app every five seconds for request counts, latency, queue depth, and forecast measurements. A scrape timestamp records when Prometheus collected the sample. A separate forecast timestamp records when the weather is predicted. Grafana uses forecast timestamps on the horizontal axis and filters by request ID so a later request for the same ZIP does not replace an earlier result's bars.
- **Docker and Oracle.** Docker Compose starts the app, Kafka, Prometheus, and Grafana. The Dockerfile compiles the Go application in a separate stage and runs the binary in the final image. The deployment uses an Ubuntu 24.04 Oracle E2.1.Micro VM with approximately 1 GB of RAM and 2 GiB of swap. The VM-specific Compose override increased Kafka's probe timeout to 60 seconds and its interval to 30 seconds. Caddy handles public HTTPS and routes `/grafana/` to Grafana. The service ports stay bound to loopback on the VM.

`main.go` starts the application. In `internal/app`, `app.go` connects the HTTP routes and services, `weather.go` retrieves weather, `jobs.go` coordinates requests, `kafka.go` handles forecast events, `alerts.go` evaluates thresholds, `metrics.go` exports measurements, and `web.go` renders the site. The `config` directory contains monitoring configuration and dashboard definitions.

## Build and run locally

Use this section to reproduce the app on your own computer. Docker Engine with Docker Compose is required. From the repository directory in PowerShell, copy the example configuration, open the copy, and set `OPENWEATHER_API_KEY` before running the Docker commands.

```powershell
if (!(Test-Path .env)) { Copy-Item .env.example .env }
notepad .env
docker compose build app
docker compose up -d --wait
docker compose ps
```

The build command compiles the Go app into an image. The up command starts that image along with Kafka, Prometheus, and Grafana. Run both commands for a local setup.

Open [the local app](http://localhost:8080/) and request five days for `12601`. To see the charts, sign in to [local Grafana](http://localhost:3000/login) in the same browser. A fresh local Grafana database uses the example credentials `admin` and `local-dev-only` unless a password is configured. The hosted instance uses its own password. The Docker build includes the Go compiler.

## Run tests locally

These commands check the source and local stack. Using the hosted site does not require them. With Go installed, `go build .` is an optional compile check. Run the automated tests from the repository root. Start the Compose stack before the Kafka integration test.

```powershell
go test -count=1 -v ./...
$env:KAFKA_TEST_BROKERS = 'localhost:9092'
go test -tags integration -run '^TestKafkaRoundTrip$' -count=1 -v ./...
```

From the same directory, run `& .\testdata\verify-live.ps1` in PowerShell 7 to submit different ZIP codes and forecast lengths together. The script polls each result and writes its inputs and outputs alongside itself. Copy the saved evidence elsewhere first if you want to preserve the original run.

## Testing and results

All 24 top-level tests in the default Go suite passed on commit `01c44a0`. They cover validation, concurrent request isolation, forecast parsing and time windows, alert boundaries, queue capacity, web output, and metrics. The Kafka integration test also passed with a real broker and controlled forecast data. It checked delivery, alert evaluation, duplicate handling, and rejection of a malformed event.

I submitted four live forecasts against the local Docker stack. All four requests overlapped on the server and finished through Kafka. I compared the forecast timestamps and values with Prometheus and confirmed that both Grafana panel queries returned the expected counts. A request with ZIP `12` and zero days returned HTTP 400.

| ZIP code | Requested days | Points per chart | Server elapsed time | Client observed time |
|---|---:|---:|---:|---:|
| `02108` | 1 | 8 | 4,548 ms | 6,740 ms |
| `60601` | 2 | 16 | 3,443 ms | 6,697 ms |
| `10001` | 3 | 24 | 5,616 ms | 8,986 ms |
| `12601` | 5 | 40 | 6,753 ms | 9,035 ms |

Server elapsed time starts at request creation and includes queueing. Client observed time also includes network and polling overhead. This run demonstrates four overlapping requests. Maximum capacity was not measured. A separate local check of the five forecast lengths returned 8, 16, 24, 32, and 40 matching points per chart for one through five days.

On Oracle, I verified the stack through an SSH tunnel first. After HTTPS was configured, a public forecast completed through Kafka and both charts loaded after signing in to Grafana. The timings above are from the local test, so they do not measure Oracle performance.

The [inputs](testdata/forecast-inputs.json), [full forecasts and timings](testdata/forecast-results.json), [Go test output](testdata/go-tests.txt), [Kafka integration output](testdata/kafka-test.txt), and [Grafana query checks](testdata/grafana-checks.json) are in `testdata/`.

## AI assistance

**Tool:** OpenAI Codex desktop app

**Model:** GPT-6.1 Sol

I used the RIC prompt below to guide the project and worked back and forth with Codex to finish the assignment. I reviewed design choices, set the alert limits, ran the app, and shared test results and Oracle setup errors. Codex helped implement the features, troubleshoot the deployment, and draft the documentation. The prompt below summarizes the initial request.

**Role:** Act as a Go and cloud computing mentor. Explain design choices and ask questions that check my understanding as we work.

**Instructions:** Use the assignment as the main guide. Ask for my input on design choices before writing an iterative plan with clear Git commit points. Help me implement and test each required feature, verify simultaneous forecasts for different ZIP codes and lengths, and deploy the finished app on Oracle Cloud. Keep the design simple and prioritize the assignment requirements.

**Context:** `cloudProject02` is a Go weather monitoring application. Users request forecasts by US ZIP code for one to five days. The project uses OpenWeather, goroutines and channels for concurrent requests, Kafka to carry forecast data to an alert evaluator, Prometheus for time-series data, and Grafana for plots.
