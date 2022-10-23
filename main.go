package main

import (
	"log"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/config"
	"github.com/Edilberto-Vazquez/weather-services/src/drivers"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

func runETL() {
	start := time.Now()

	log.Printf("Loading efm events from: %s", config.EVENTS_FILE_PATH)
	err := usecases.LoadEFMEventLogs(config.EVENTS_FILE_PATH)
	if err != nil {
		log.Fatalf("Could not loading efm events; Error: %s", err.Error())
	}

	log.Printf("Loading efm file paths from: %s", config.EFM_FILES_PATH)
	efmFilePaths, err := utils.ReadDirectory(config.EFM_FILES_PATH, "efm")
	if err != nil {
		log.Fatalf("Could not loading files paths from: %s; Error: %s", config.EFM_FILES_PATH, err.Error())
	}

	repo := drivers.NewMongoDBConnection(config.DB_CONFIG)
	pipeline := usecases.GetEFMETLPipeline()
	wpConfig := usecases.NewETLWorkerPoolConfig(config.WORKERS, repo, efmFilePaths, pipeline)
	wp := usecases.NewETLWorkerPool(wpConfig)
	wp.Run()

	weatherFilePaths, err := utils.ReadDirectory(config.WEATHER_FILES_PATH, "csv")
	if err != nil {
		log.Fatalf("Could not loading weather file paths from: %s; Error: %s", config.WEATHER_FILES_PATH, err.Error())
	}
	pipeline = usecases.GetWeatherETLPipeline()
	wp.SetPipeline(pipeline)
	wp.SetFiles(weatherFilePaths)
	wp.Run()

	duration := time.Since(start)
	log.Println(duration)
}

func main() {
	config.LoadConfig()
	runETL()
}
