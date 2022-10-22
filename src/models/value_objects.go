package models

import "github.com/Edilberto-Vazquez/weather-services/src/repository"

type ETLPipeline interface {
	Extract(filePath string) error
	Transform()
	Load(repo repository.Repository) error
}

type NewETLPipeline func() ETLPipeline

type DBConfig struct {
	URI        string
	Name       string
	Collection string
}

type EFMLogEvent struct {
	DateTime  string
	Lightning bool
	Distance  uint8
}

type EFMLogEvents map[string]EFMLogEvent

type EFMConfig struct {
	EFMLogEvents EFMLogEvents
	EFMFiles     []string
	EFMFilesChan chan string
}
