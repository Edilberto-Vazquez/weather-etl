package drivers

import (
	"context"
	"log"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/config"
	"github.com/Edilberto-Vazquez/weather-services/src/models"
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

func (m *MongoDBDriver) InsertEFMRecords(records []interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if records == nil {
		return nil
	}
	_, err := m.db.Collection(config.DB_EFM_COLLECTION).InsertMany(ctx, records)
	if err != nil {
		return err
	}
	return nil
}

func (m *MongoDBDriver) InsertWeatherRecords(records []interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if records == nil {
		return nil
	}
	_, err := m.db.Collection(config.DB_WEATHER_COLLECTION).InsertMany(ctx, records)
	if err != nil {
		return err
	}
	return nil
}
