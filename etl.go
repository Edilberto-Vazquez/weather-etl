package main

import (
	"log"
	"sync"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/drivers"
	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases/extract"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases/transform"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

var (
	EFMLogEvents = make(models.EFMLogEvents)
)

func etlWorker(files <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	m := drivers.MongoDBConnection()
	for file := range files {
		log.Printf("Extracting: %s", file)
		electricFields, err := extract.EFMElectricFieldsExtraction(file)
		if err != nil {
			log.Printf("Error extracting: %s; Error: %s\n", file, err.Error())
			continue
		}
		log.Printf("Extracted: %s", file)

		log.Printf("Transforming: %s", electricFields.FileName)
		transformedLines := transform.TransformEFMLines(EFMLogEvents, electricFields)
		log.Printf("Transformed: %s", transformedLines.FileName)

		log.Printf("Loading: %s", transformedLines.FileName)
		err = m.InsertTransformedLines(transformedLines)
		if err != nil {
			log.Printf("Error loading: %s; Error: %s\n", transformedLines.FileName, err.Error())
			continue
		}
		log.Printf("Loaded: %s", transformedLines.FileName)
	}
}

func main() {
	workers := 10
	var wg sync.WaitGroup
	start := time.Now()

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
	filesPipeline := make(chan string, len(efmFilesPath))
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go etlWorker(filesPipeline, &wg)
	}
	for _, filePath := range efmFilesPath {
		filesPipeline <- filePath
	}
	close(filesPipeline)
	wg.Wait()
	duration := time.Since(start)
	log.Println(duration)
}
