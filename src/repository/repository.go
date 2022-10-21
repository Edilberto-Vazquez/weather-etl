package repository

type Repository interface {
	InsertTransformedLines(transformedLines []interface{}) error
}
