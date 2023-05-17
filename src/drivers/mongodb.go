package drivers

import (
	"context"
	"log"

	"github.com/Edilberto-Vazquez/weather-etl/src/config"
	"github.com/Edilberto-Vazquez/weather-etl/src/models"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBDriver struct {
	db *mongo.Database
}

func NewMongoDBConnection(dbConfig models.DBConfig) *MongoDBDriver {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(dbConfig.URI))
	if err != nil {
		log.Fatal("Could not connect to mongoDB")
	}
	db := client.Database(dbConfig.Name)
	return &MongoDBDriver{db}
}

func (m *MongoDBDriver) InsertEFMRecords(records []interface{}, ctx context.Context) error {
	if records == nil {
		return nil
	}
	_, err := m.db.Collection(config.DB_EFM_COLLECTION).InsertMany(ctx, records)
	if err != nil {
		return err
	}
	return nil
}

func (m *MongoDBDriver) InsertWeatherRecords(records []interface{}, ctx context.Context) error {
	if records == nil {
		return nil
	}
	_, err := m.db.Collection(config.DB_WEATHER_COLLECTION).InsertMany(ctx, records)
	if err != nil {
		return err
	}
	return nil
}
