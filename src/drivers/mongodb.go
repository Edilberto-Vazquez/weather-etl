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

func MongoDBConnection() MongoDBDriver {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://root:root@localhost:27017/efm-stations"))
	if err != nil {
		log.Panic("Could not connect to mongoDB")
	}
	db := client.Database("efm-stations")
	collection := db.Collection("electric-fields")
	return MongoDBDriver{db, collection}
}

func (m *MongoDBDriver) InsertTransformedLines(transformedLines *models.EFMTransformedLines) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if transformedLines == nil {
		return nil
	}
	log.Printf("Loading %s", transformedLines.FileName)
	_, err := m.collection.InsertMany(ctx, transformedLines.TransformedLines)
	if err != nil {
		log.Printf("Could not load %s", transformedLines.FileName)
		return err
	}
	log.Printf("Loaded %s", transformedLines.FileName)
	return nil
}
