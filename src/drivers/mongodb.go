package drivers

import (
	"context"
	"log"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBDriver struct {
	db         *mongo.Database
	collection *mongo.Collection
}

func NewMongoDBConnection(dbConfig models.DBConfig) *MongoDBDriver {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(dbConfig.URI))
	if err != nil {
		log.Fatal("Could not connect to mongoDB")
	}
	db := client.Database(dbConfig.Name)
	collection := db.Collection(dbConfig.Collection)
	return &MongoDBDriver{db, collection}
}

func (m *MongoDBDriver) InsertTransformedLines(transformedLines []interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if transformedLines == nil {
		return nil
	}
	_, err := m.collection.InsertMany(ctx, transformedLines)
	if err != nil {
		return err
	}
	return nil
}
