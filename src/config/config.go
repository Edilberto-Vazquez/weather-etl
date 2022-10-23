package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/joho/godotenv"
)

const (
	APP_NAME              string = "EFM-ETL"
	DB_EFM_COLLECTION     string = "ElectricFields"
	DB_WEATHER_COLLECTION string = "WeatherRecords"
)

var (
	WORKERS            int             = 4
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

	// Load env vars
	err := godotenv.Load()
	if err != nil {
		return err
	}

	// Check env vars
	for k := range envVars {
		if os.Getenv(k) == "" {
			return fmt.Errorf("error to load %s variable from .env file you need provide a value", k)
		}
		envVars[k] = os.Getenv(k)
	}

	// Set number of workers
	workers, err := strconv.Atoi(envVars["WORKERS"])
	if err != nil {
		return errors.New("error to load workers number from .env file using default number of workers(4)")
	} else {
		WORKERS = workers
	}

	// Set DB config
	DB_CONFIG = models.DBConfig{URI: envVars["DB_URI"], Name: envVars["DB_NAME"]}

	// Set file paths
	EVENTS_FILE_PATH = envVars["EVENTS_FILE_PATH"]
	EFM_FILES_PATH = envVars["EFM_FILES_PATH"]
	WEATHER_FILES_PATH = envVars["WEATHER_FILES_PATH"]

	return nil
}
