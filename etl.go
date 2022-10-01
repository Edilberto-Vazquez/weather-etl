package main

import (
	"log"

	"github.com/Edilberto-Vazquez/weather-services/src/drivers"
	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases/extract"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases/transform"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

var (
	EFMLogEvents = make(models.EFMLogEvents)
)

func extractWorker(extractPipeline <-chan string, transformPipeline chan<- *models.EFMElectricFields) {
	for path := range extractPipeline {
		log.Printf("Extracting: %s", path)
		electricFields, err := extract.EFMElectricFieldsExtraction(path)
		if err != nil {
			log.Printf("Error extracting: %s; Error: %s\n", path, err.Error())
			transformPipeline <- nil
			continue
		}
		transformPipeline <- electricFields
		log.Printf("Extracted: %s", path)
	}
}

func transformWorker(transformPipeline <-chan *models.EFMElectricFields, loadPipeline chan<- *models.EFMTransformedLines) {
	for electricFields := range transformPipeline {
		if electricFields == nil {
			loadPipeline <- nil
			continue
		}
		log.Printf("Transforming: %s", electricFields.FileName)
		processedLines := transform.TransformEFMLines(EFMLogEvents, electricFields)
		loadPipeline <- processedLines
		log.Printf("Transformed: %s", electricFields.FileName)
	}
}

func main() {
	workers := 10
	m := drivers.MongoDBConnection()

	log.Printf("Extracting event logs from: %s", "./etl-test-files/EFMEvents.log")
	err := extract.EFMEeventLogExtraction("./etl-test-files/EFMEvents.log", EFMLogEvents)
	if err != nil {
		log.Panicf("Could not extract events from: %s; Error: %s", "./etl-test-files/EFMEvents.log", err.Error())
	}
	log.Printf("Event logs extracted from: %s", "./etl-test-files/EFMEvents.log")

	log.Printf("Reading efm files from: %s", "/home/potatofy/campo-electrico")
	efmFilesPath, err := utils.ReadDirectory("/home/potatofy/campo-electrico", "efm")
	if err != nil {
		log.Panicf("Could not read directory: %s", "/home/potatofy/campo-electrico")
	}
	extractPipeline := make(chan string, len(efmFilesPath))
	transformPipeline := make(chan *models.EFMElectricFields, 10)
	loadPipeline := make(chan *models.EFMTransformedLines, 1)
	for i := 0; i < workers; i++ {
		go extractWorker(extractPipeline, transformPipeline)
		go transformWorker(transformPipeline, loadPipeline)
	}
	for _, filePath := range efmFilesPath {
		extractPipeline <- filePath
	}
	close(extractPipeline)

	for i := 0; i < len(efmFilesPath); i++ {
		m.InsertTransformedLines(<-loadPipeline)
	}
	close(loadPipeline)
}
