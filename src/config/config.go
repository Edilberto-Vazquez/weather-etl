package config

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/Edilberto-Vazquez/weather-etl/src/models"
	"github.com/joho/godotenv"
)

const (
	APP_NAME              string = "EFM-ETL"
	DB_EFM_COLLECTION     string = "EFMRecords"
	DB_WEATHER_COLLECTION string = "WeatherRecords"
)

var (
	WORKERS            int64           = 4
	DB_CONFIG          models.DBConfig = models.DBConfig{URI: "", Name: ""}
	EVENTS_FILE_PATH   string          = ""
	EFM_FILES_PATH     string          = ""
	WEATHER_FILES_PATH string          = ""
)

func LoadConfig() error {
	envVars := map[string]string{
		"WORKERS":            "",
		"DB_URI":             "",
		"DB_NAME":            "",
		"EVENTS_FILE_PATH":   "",
		"EFM_FILES_PATH":     "",
		"WEATHER_FILES_PATH": "",
	}

	log.Println("Loading config from .env file")

	// Load env vars
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("[CONFIG] fail to read .env file; error: %s", err.Error())
	}

	// Check env vars
	for k := range envVars {
		if os.Getenv(k) == "" {
			log.Fatalf("[CONFIG] fail to load variable (%s) from .env file you need provide a value", k)
		}
		envVars[k] = os.Getenv(k)
	}

	// Set number of workers
	workers, err := strconv.ParseInt(envVars["WORKERS"], 10, 64)
	if err != nil {
		log.Printf("[CONFIG] fail to load variable (WORKERS) from .env file; error %s", err)

		var numberOfWorkers int64
		var answer string
		fmt.Print("[CONFIG] You want use default number of workers(4) yes/no: ")
		answers := map[string]bool{"Yes": true, "yes": true, "Y": true, "y": true, "": true}
		fmt.Scanf("%s", &answer)
		if answers[answer] {
			log.Printf("[CONFIG] using default number of workers (%d)\n", WORKERS)
		} else {
			fmt.Print("[CONFIG]: enter the number of workers to use: ")
			fmt.Scanf("%d", &numberOfWorkers)
			WORKERS = numberOfWorkers
		}
	} else {
		WORKERS = workers
	}

	// Set DB config
	DB_CONFIG = models.DBConfig{URI: envVars["DB_URI"], Name: envVars["DB_NAME"]}

	// Set file paths
	EVENTS_FILE_PATH = envVars["EVENTS_FILE_PATH"]
	EFM_FILES_PATH = envVars["EFM_FILES_PATH"]
	WEATHER_FILES_PATH = envVars["WEATHER_FILES_PATH"]

	log.Println("[CONFIG] Config Loaded from .env file")

	return nil
}
