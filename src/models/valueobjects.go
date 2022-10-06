package models

type EFMLogEvent struct {
	DateTime  string
	Lightning bool
	Distance  uint8
}

type EFMLogEvents map[string]EFMLogEvent
