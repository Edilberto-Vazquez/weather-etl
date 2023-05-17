package models

import (
	"context"

	"github.com/Edilberto-Vazquez/weather-etl/src/repository"
)

type DBConfig struct {
	URI  string
	Name string
}

type ETLPipeline interface {
	Extract() (extractedRecords []string, err error)
	Transform(records []string) (transformedRecords []interface{}, err error)
	Load(records []interface{}, ctx context.Context) error
	RunETL(ctx context.Context) error
}

type NewETLPipeline func(filePath string, repo repository.Repository) ETLPipeline
