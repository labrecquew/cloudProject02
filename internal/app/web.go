package app

import (
	"bytes"
	_ "embed"
	"errors"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
)

// Templates and CSS are compiled into the binary, including the Docker image.
//
//go:embed web/page.html
var pageHTML string

//go:embed web/style.css
var pageCSS []byte

const forecastTimeZoneName = "America/New_York"

var forecastTimeZone = loadForecastTimeZone()

func loadForecastTimeZone() *time.Location {
	location, err := time.LoadLocation(forecastTimeZoneName)
	if err != nil {
		panic(err)
	}
	return location
}

var webTemplate = template.Must(template.New("page").Funcs(template.FuncMap{
	"number": func(value float64) string { return strconv.FormatFloat(value, 'f', 1, 64) },
	"when":   func(at time.Time) string { return at.In(forecastTimeZone).Format("Mon, Jan 2 · 3:04 PM MST") },
	"clock":  func(at time.Time) string { return at.In(forecastTimeZone).Format("3:04 PM MST") },
	"date":   func(at time.Time) string { return at.In(forecastTimeZone).Format("Jan 2") },
}).Parse(pageHTML))

type displayPoint struct {
	Time              time.Time
	Temperature, Wind float64
	Alerts            []string
}

type displayDay struct {
	Label  string
	Points []displayPoint
}

type displayAlert struct {
	Name, Unit, Comparison string
	Time                   time.Time
	Value, Threshold       float64
}
type displaySummary struct {
	High, Low, Wind float64
	Location        string
	Start, End      time.Time
	Count           int
}
type webPage struct {
	ZIP                                        string
	Days                                       int
	DayOptions                                 []int
	Configured, Pending, About                 bool
	Error, Status, Stage                       string
	Job                                        *Job
	Summary                                    *displaySummary
	ForecastDays                               []displayDay
	Alerts                                     []displayAlert
	HighThreshold, LowThreshold, WindThreshold float64

	TemperatureChart, WindChart string
}

func fahrenheit(c float64) float64     { return c*9/5 + 32 }
func milesPerHour(mps float64) float64 { return mps * 2.2369362920544 }

func dashboardLink(r *http.Request, dashboard, zip string) string {
	base, err := url.Parse(strings.TrimSpace(os.Getenv("GRAFANA_PUBLIC_URL")))
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") || base.User != nil {
		host := r.Host
		if parsed, _, err := net.SplitHostPort(host); err == nil {
			host = parsed
		}
		host = strings.Trim(host, "[]")
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		base = &url.URL{Scheme: scheme, Host: net.JoinHostPort(host, "3000")}
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/d/" + dashboard + "/" + dashboard
	base.RawQuery, base.Fragment = "", ""
	if zip != "" {
		query := url.Values{"var-zip": {zip}}
		base.RawQuery = query.Encode()
	}
	return base.String()
}

func forecastPanelURL(dashboard *url.URL, panelID, requestID string) string {
	panel := *dashboard
	panel.Path = strings.TrimSuffix(panel.Path, "/d/weather-forecasts/weather-forecasts") + "/d-solo/weather-forecasts/weather-forecasts"
	query := panel.Query()
	query.Set("panelId", panelID)
	query.Set("var-forecast_request", requestID)
	query.Set("theme", "light")
	query.Set("timezone", forecastTimeZoneName)
	query.Set("refresh", "10s")
	panel.RawQuery = query.Encode()
	return panel.String()
}

func newWebPage(jobs *jobService, configured bool) webPage {
	return webPage{Days: 3, DayOptions: []int{1, 2, 3, 4, 5}, Configured: configured,
		HighThreshold: fahrenheit(jobs.rules.HighTempC), LowThreshold: fahrenheit(jobs.rules.LowTempC), WindThreshold: milesPerHour(jobs.rules.HighWind)}
}

func renderPage(w http.ResponseWriter, status int, page webPage) {
	var body bytes.Buffer
	if err := webTemplate.ExecuteTemplate(&body, "page", page); err != nil {
		slog.Error("render web page", "error", err)
		http.Error(w, "Could not display this page. Please try again.", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "same-origin")
	policy := "default-src 'none'; style-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
	if page.TemperatureChart != "" {
		chart, err := url.Parse(page.TemperatureChart)
		if err == nil && !strings.ContainsAny(chart.Host, ";,") {
			policy += "; frame-src " + chart.Scheme + "://" + chart.Host
		}
	}
	w.Header().Set("Content-Security-Policy", policy)
	w.WriteHeader(status)
	w.Write(body.Bytes())
}

func registerWeb(mux *http.ServeMux, jobs *jobService, configured bool) {
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { renderPage(w, 200, newWebPage(jobs, configured)) })
	mux.HandleFunc("GET /about", func(w http.ResponseWriter, r *http.Request) {
		page := newWebPage(jobs, configured)
		page.About = true
		renderPage(w, 200, page)
	})
	mux.HandleFunc("GET /static/style.css", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Write(pageCSS)
	})
	protection := http.NewCrossOriginProtection()
	protection.SetDenyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := newWebPage(jobs, configured)
		page.Error = "Please submit your request from this website."
		renderPage(w, http.StatusForbidden, page)
	}))
	mux.Handle("POST /request", protection.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := newWebPage(jobs, configured)
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		if err := r.ParseForm(); err != nil {
			page.Error = "We could not read that request. Please enter a ZIP code and forecast length."
			renderPage(w, 400, page)
			return
		}
		page.ZIP = strings.TrimSpace(r.PostForm.Get("zip"))
		days, err := strconv.Atoi(r.PostForm.Get("days"))
		if days >= 1 && days <= 5 {
			page.Days = days
		}
		input := ForecastRequest{ZIP: page.ZIP, Days: days}
		if err != nil {
			page.Error = "Choose a forecast length from 1 to 5 days."
		} else if err := input.validate(); err != nil {
			page.Error = err.Error()
		}
		if page.Error != "" {
			renderPage(w, 400, page)
			return
		}
		if !configured {
			page.Error = "Forecast requests are temporarily unavailable. Please try again later."
			renderPage(w, 503, page)
			return
		}
		job, err := jobs.submit(input)
		if err != nil {
			page.Error = "We could not start your forecast. Please try again."
			if errors.Is(err, errBusy) {
				page.Error = "All forecast slots are busy. Please try again shortly."
				w.Header().Set("Retry-After", "5")
				renderPage(w, 503, page)
			} else {
				renderPage(w, 500, page)
			}
			return
		}
		http.Redirect(w, r, "/requests/"+job.ID, http.StatusSeeOther)
	})))
	mux.HandleFunc("GET /requests/{id}", func(w http.ResponseWriter, r *http.Request) {
		page := newWebPage(jobs, configured)
		job, ok := jobs.lookup(r.PathValue("id"))
		if !ok {
			page.Error = "This forecast is no longer available. Please request a new forecast."
			renderPage(w, 404, page)
			return
		}
		page.Job = &job
		page.ZIP = job.Request.ZIP
		page.Days = job.Request.Days

		switch job.Status {
		case "queued":
			page.Pending = true
			page.Status = "Request waiting"
			page.Stage = "Your forecast request will start shortly."
		case "running":
			page.Pending = true
			page.Status = "Getting forecast"
			page.Stage = "Retrieving your weather forecast."
		case "awaiting_evaluation":
			page.Pending = true
			page.Status = "Checking alerts"
			page.Stage = "Checking for high temperature, low temperature, and high wind."
		case "failed":
			page.Status = "Forecast unavailable"
			page.Error = job.Error
		case "completed":
			if job.Forecast == nil || len(job.Forecast.Points) == 0 {
				page.Error = "This forecast could not be displayed. Please request another."
				renderPage(w, 500, page)
				return
			}
			f := job.Forecast
			summary := displaySummary{Location: f.Location, Count: len(f.Points), Start: f.Points[0].Time, End: f.Points[len(f.Points)-1].Time, High: fahrenheit(f.Points[0].TemperatureCelsius), Low: fahrenheit(f.Points[0].TemperatureCelsius)}
			labels := map[time.Time][]string{}
			for _, alert := range job.Alerts {
				item := displayAlert{Time: alert.Time, Value: fahrenheit(alert.Value), Threshold: fahrenheit(alert.Threshold), Unit: "°F", Comparison: "at or above"}
				switch alert.Type {
				case "high_temperature":
					item.Name = "High temperature"
				case "low_temperature":
					item.Name = "Low temperature"
					item.Comparison = "at or below"
				case "high_wind":
					item.Name = "High wind"
					item.Value = milesPerHour(alert.Value)
					item.Threshold = milesPerHour(alert.Threshold)
					item.Unit = "mph"
				}
				page.Alerts = append(page.Alerts, item)
				labels[alert.Time] = append(labels[alert.Time], item.Name)
			}
			for _, point := range f.Points {
				temp, wind := fahrenheit(point.TemperatureCelsius), milesPerHour(point.WindMetersPerSec)
				summary.High = max(summary.High, temp)
				summary.Low = min(summary.Low, temp)
				summary.Wind = max(summary.Wind, wind)
				dayLabel := point.Time.In(forecastTimeZone).Format("Monday, January 2")
				last := len(page.ForecastDays) - 1
				if last < 0 || page.ForecastDays[last].Label != dayLabel {
					page.ForecastDays = append(page.ForecastDays, displayDay{Label: dayLabel})
					last++
				}
				day := &page.ForecastDays[last]
				day.Points = append(day.Points, displayPoint{point.Time, temp, wind, labels[point.Time]})
			}
			page.Summary = &summary
			if dashboard, err := url.Parse(dashboardLink(r, "weather-forecasts", page.ZIP)); err == nil {
				page.TemperatureChart = forecastPanelURL(dashboard, "8", job.ID)
				page.WindChart = forecastPanelURL(dashboard, "9", job.ID)
			}
		}
		renderPage(w, 200, page)
	})
}
