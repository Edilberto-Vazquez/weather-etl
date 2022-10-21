package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/constants"
	"github.com/Edilberto-Vazquez/weather-services/src/drivers"
	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
	"github.com/joho/godotenv"
)

// func usage() {
// 	msg := fmt.Sprintf(`usage: %s [OPTIONS]%s ETL for EFM-100 Atmospheric Electric Field Monitor`, constants.AppName, constants.AppName)
// 	fmt.Println(msg)
// 	flag.PrintDefaults()
// }

func runETL(workers int, eventsFilePath, efmFilesPath string, dbConfig models.DBConfig) {
	start := time.Now()

	repo := drivers.NewMongoDBConnection(dbConfig)
	pipeline := usecases.GetEFMETLPipeline()

	log.Printf("Loading efm file paths from: %s", efmFilesPath)
	efmFiles, err := utils.ReadDirectory(efmFilesPath, "efm")
	if err != nil {
		log.Fatalf("Could not loading files paths from: %s; Error: %s", efmFilesPath, err.Error())
	}

	config := usecases.NewETLWorkerPoolConfig(workers, repo, efmFiles, pipeline)
	wp := usecases.NewETLWorkerPool(config)
	usecases.SetEFMEventLogs(eventsFilePath)
	wp.Run()

	duration := time.Since(start)
	log.Println(duration)
}

func main() {
	envVars := map[string]string{
		"WORKERS":          "",
		"EVENTS_FILE_PATH": "",
		"EFM_FILES_PATH":   "",
		"DB_URI":           "",
		"DB_NAME":          "",
		"DB_COLLECTION":    "",
	}
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %s", err.Error())
	}

	for k, _ := range envVars {
		if os.Getenv(k) == "" {
			log.Fatalf("Error to load %s variable from .env file you need provide a value", k)
			continue
		}
		envVars[k] = os.Getenv(k)
	}

	workers, err := strconv.Atoi(envVars["WORKERS"])
	if err != nil {
		log.Println("Error to load workers number from .env file loading default config")
		workers = constants.Workers
	}
	eventsFilePath := envVars["EVENTS_FILE_PATH"]
	efmFilesPath := envVars["EFM_FILES_PATH"]
	dbConfig := models.DBConfig{
		URI:        envVars["DB_URI"],
		Name:       envVars["DB_NAME"],
		Collection: envVars["DB_COLLECTION"],
	}

	runETL(workers, eventsFilePath, efmFilesPath, dbConfig)

	// workers := flag.Int("workers", constants.Workers, "The number of workers that are processing the information (Workers should be equal to or less than CPU cores for best performance)")
	// eventsFile := flag.String("events-file", "", "Path of the file with the EFM event logs")
	// efmFiles := flag.String("efm-files", "", "Path of the folder with the EFM files")
	// dbURI := flag.String("db-uri", "", "URI of the DB where the data will be stored")
	// flag.Usage = usage
	// flag.Parse()
	// if *eventsFile != "" && *efmFiles != "" && *dbURI != "" {
	// 	runETL(*workers, *eventsFile, *efmFiles, *dbURI)
	// } else {
	// 	log.Println("-events-file or -efm-files parameter not passed, loading configuration from etl.conf file")
	// 	file, err := utils.OpenFile("./etl.conf")
	// 	if err != nil {
	// 		file.Close()
	// 		log.Fatalf("Could not open the etl.conf file Error: %s", err.Error())
	// 	}
	// 	scanner := bufio.NewScanner(file)
	// 	for scanner.Scan() {
	// 		parameter := strings.Split(scanner.Text(), "=")
	// 		log.Println(scanner.Text())
	// 		if parameter[1] == "" {
	// 			log.Fatalf("Could not load the parameter %s from etl.conf value not found.\nAdd a value to the parameter %s in etl.conf or pass the flag -%s.\n--help for see all options", parameter[0], parameter[0], parameter[0])
	// 		}
	// 		switch parameter[0] {
	// 		case "workers":
	// 			v, _ := strconv.Atoi(parameter[1])
	// 			workers = &v
	// 		case "events-file":
	// 			eventsFile = &parameter[1]
	// 		case "efm-files":
	// 			efmFiles = &parameter[1]
	// 		case "db-uri":
	// 			dbURI = &parameter[1]
	// 		}
	// 	}
	// 	file.Close()
}
