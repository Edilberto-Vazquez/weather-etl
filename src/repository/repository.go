package repository

import "context"

type Repository interface {
	InsertEFMRecords(records []interface{}, ctx context.Context) error
	InsertWeatherRecords(records []interface{}, ctx context.Context) error
}
