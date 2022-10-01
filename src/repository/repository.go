package repository

import "github.com/Edilberto-Vazquez/weather-services/src/models"

type MongoDBRepository interface {
	InsertTransformedLines(transformedLines *models.EFMTransformedLines) error
}
