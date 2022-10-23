package models

import "github.com/Edilberto-Vazquez/weather-services/src/repository"

type DBConfig struct {
	URI  string
	Name string
}

type ETLPipeline interface {
	Extract() (extractedRecords []string, err error)
	Transform(records []string) (transformedRecords []interface{})
	Load(records []interface{}) error
	RunETL() error
}

type NewETLPipeline func(filePath string, repo repository.Repository) ETLPipeline
