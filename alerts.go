package main

import (
	"errors"
	"math"
	"time"
)

// 90 degrees Fahrenheit, represented in Celsius for internal comparisons.
const defaultHighTempC = (90.0 - 32) * 5 / 9

type alertRules struct {
	HighTempC float64 `json:"high_temperature_celsius"`
	LowTempC  float64 `json:"low_temperature_celsius"`
	HighWind  float64 `json:"high_wind_meters_per_second"`
}

func (r alertRules) validate() error {
	for _, n := range []float64{r.HighTempC, r.LowTempC, r.HighWind} {
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return errors.New("alert thresholds must be finite numbers")
		}
	}
	if r.LowTempC >= r.HighTempC || r.HighWind < 0 {
		return errors.New("low temperature must be below high temperature, and wind threshold must be nonnegative")
	}
	return nil
}

type Alert struct {
	Type      string    `json:"type"`
	Time      time.Time `json:"time"`
	Value     float64   `json:"value"`
	Threshold float64   `json:"threshold"`
	Unit      string    `json:"unit"`
}

// Each matching forecast point produces an alert. Equality triggers an alert.
func (r alertRules) evaluate(f Forecast) []Alert {
	alerts := make([]Alert, 0)
	for _, point := range f.Points {
		if point.TemperatureCelsius >= r.HighTempC {
			alerts = append(alerts, Alert{"high_temperature", point.Time, point.TemperatureCelsius, r.HighTempC, "celsius"})
		}
		if point.TemperatureCelsius <= r.LowTempC {
			alerts = append(alerts, Alert{"low_temperature", point.Time, point.TemperatureCelsius, r.LowTempC, "celsius"})
		}
		if point.WindMetersPerSec >= r.HighWind {
			alerts = append(alerts, Alert{"high_wind", point.Time, point.WindMetersPerSec, r.HighWind, "meters_per_second"})
		}
	}
	return alerts
}
