package main

import (
	"time"
)

type SensorData struct {
	Temperature float64   `json:"temperature"`
	Voltage     float64   `json:"voltage"`
	Timestamp   time.Time `json:"timestamp"`
}
