package main

type Record struct {
	UUID string `json:"uuid"`

	Locale   string `json:"locale"`
	Language string `json:"language"`

	Country string `json:"country"`
	Region  string `json:"region"`
	City    string `json:"city"`

	IP string `json:"ip"`

	IsMetric bool `json:"metric" db:"is_metric"`

	saved bool // for internal usage in checker
}
