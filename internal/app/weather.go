package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"
)

type ForecastRequest struct {
	ZIP  string `json:"zip"`
	Days int    `json:"days"`
}

func (r ForecastRequest) validate() error {
	if len(r.ZIP) != 5 {
		return errors.New("zip must be a five-digit US ZIP code")
	}
	for _, digit := range r.ZIP {
		if digit < '0' || digit > '9' {
			return errors.New("zip must be a five-digit US ZIP code")
		}
	}
	if r.Days < 1 || r.Days > 5 {
		return errors.New("days must be between 1 and 5")
	}
	return nil
}

type ForecastPoint struct {
	Time               time.Time `json:"time"`
	TemperatureCelsius float64   `json:"temperature_celsius"`
	WindMetersPerSec   float64   `json:"wind_meters_per_second"`
}

type Forecast struct {
	ZIP           string          `json:"zip"`
	Location      string          `json:"location"`
	Days          int             `json:"days"`
	WindowStart   time.Time       `json:"window_start"`
	WindowEnd     time.Time       `json:"window_end"`
	CoverageStart time.Time       `json:"coverage_start"`
	CoverageEnd   time.Time       `json:"coverage_end"`
	Points        []ForecastPoint `json:"points"`
}

type forecastProvider interface {
	Fetch(context.Context, ForecastRequest) (Forecast, error)
}

type openWeather struct {
	key    string
	client *http.Client
	base   string
	ticks  <-chan time.Time
}

// All workers share this provider and its ticker: at most one outgoing API call
// per tick, including geocoding. Tests can supply no ticker to avoid real delays.
func (p *openWeather) get(ctx context.Context, path string, query url.Values, out any) error {
	if p.ticks != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.ticks:
		}
	}
	query.Set("appid", p.key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+path+"?"+query.Encode(), nil)
	if err != nil {
		return errors.New("could not construct weather request")
	}
	resp, err := p.client.Do(req)
	if err != nil {
		// HTTP errors can contain the URL and its API key. Never return them raw.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("could not reach OpenWeather; try again later")
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		return errors.New("OpenWeather rejected the API key or this account's API access")
	case http.StatusNotFound:
		return errors.New("OpenWeather could not find that US ZIP code")
	case http.StatusTooManyRequests:
		return errors.New("OpenWeather rate limit reached; try again later")
	default:
		return fmt.Errorf("OpenWeather returned HTTP %d; try again later", resp.StatusCode)
	}
	const limit = 2 * 1024 * 1024
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil || len(body) > limit {
		return errors.New("could not read weather response")
	}
	if json.Unmarshal(body, out) != nil {
		return errors.New("OpenWeather returned an invalid JSON response")
	}
	return nil
}

func (p *openWeather) Fetch(ctx context.Context, input ForecastRequest) (Forecast, error) {
	if err := input.validate(); err != nil {
		return Forecast{}, err
	}
	if p.key == "" {
		return Forecast{}, errors.New("OPENWEATHER_API_KEY is not configured")
	}
	start := time.Now().UTC()
	var location struct {
		ZIP     string   `json:"zip"`
		Name    string   `json:"name"`
		Lat     *float64 `json:"lat"`
		Lon     *float64 `json:"lon"`
		Country string   `json:"country"`
	}
	if err := p.get(ctx, "/geo/1.0/zip", url.Values{"zip": {input.ZIP + ",US"}}, &location); err != nil {
		return Forecast{}, err
	}
	if location.Lat == nil || location.Lon == nil || location.ZIP != input.ZIP || location.Country != "US" ||
		*location.Lat < -90 || *location.Lat > 90 || *location.Lon < -180 || *location.Lon > 180 {
		return Forecast{}, errors.New("OpenWeather returned an invalid ZIP location")
	}
	var response struct {
		List []struct {
			DT   int64 `json:"dt"`
			Main struct {
				Temp *float64 `json:"temp"`
			} `json:"main"`
			Wind struct {
				Speed *float64 `json:"speed"`
			} `json:"wind"`
		} `json:"list"`
	}
	query := url.Values{
		"lat":   {strconv.FormatFloat(*location.Lat, 'f', -1, 64)},
		"lon":   {strconv.FormatFloat(*location.Lon, 'f', -1, 64)},
		"units": {"metric"},
	}
	if err := p.get(ctx, "/data/2.5/forecast", query, &response); err != nil {
		return Forecast{}, err
	}
	forecast := Forecast{
		ZIP: input.ZIP, Location: location.Name, Days: input.Days,
		WindowStart: start, WindowEnd: start.Add(time.Duration(input.Days) * 24 * time.Hour),
		Points: make([]ForecastPoint, 0),
	}
	for _, item := range response.List {
		if item.DT <= 0 || item.Main.Temp == nil || item.Wind.Speed == nil || *item.Wind.Speed < 0 {
			return Forecast{}, errors.New("OpenWeather returned incomplete forecast data")
		}
		at := time.Unix(item.DT, 0).UTC()
		if at.Before(start) || !at.Before(forecast.WindowEnd) {
			continue
		}
		forecast.Points = append(forecast.Points, ForecastPoint{at, *item.Main.Temp, *item.Wind.Speed})
	}
	if len(forecast.Points) == 0 {
		return Forecast{}, errors.New("OpenWeather returned no forecasts in the requested time window")
	}
	sort.Slice(forecast.Points, func(i, j int) bool { return forecast.Points[i].Time.Before(forecast.Points[j].Time) })
	forecast.CoverageStart = forecast.Points[0].Time
	forecast.CoverageEnd = forecast.Points[len(forecast.Points)-1].Time
	return forecast, nil
}
