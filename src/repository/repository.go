package repository

type EFMRepository interface {
	InsertTransformedLines(transformedLines []interface{}) error
}
