package repository

type Repository interface {
	InsertEFMRecords(records []interface{}) error
	InsertWeatherRecords(records []interface{}) error
}
