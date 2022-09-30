package main

import (
	"log"
	"sync"

	"github.com/Edilberto-Vazquez/weather-services/src/usecases/extract"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases/transform"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

func EfmEtl(filePath string, mux *sync.RWMutex) (transformedFile []*transform.TransformedElectricFieldLine, err error) {
	log.Printf("[ELECTRIC_FIELD_ETL] Extracting: %s", filePath)
	extFile, err := extract.ExtractFile(filePath)
	if err != nil {
		log.Printf("[ELECTRIC_FIELD_ETL] Error extracting: %s; error: %s\n", filePath, err.Error())
		return nil, err
	}
	transformedFile, err = transform.TransformElectricFieldFile(extFile, mux)
	if err != nil {
		log.Printf("[ELECTRIC_FIELD_ETL] Error transforming: %s; error: %s\n", filePath, err.Error())
		return nil, err
	}
	log.Printf("[ELECTRIC_FIELD_ETL] Processed successfully: %s\n", filePath)
	return transformedFile, nil
}

func worker(loadFile <-chan string, transformFile chan<- []*transform.TransformedElectricFieldLine, mux *sync.RWMutex) {
	for file := range loadFile {
		tf, err := EfmEtl(file, mux)
		if err != nil || tf == nil {
			continue
		}
		mux.Lock()
		transformFile <- tf
		mux.Unlock()
	}
}

func main() {
	workers := 8
	// var mux *sync.RWMutex
	mux := &sync.RWMutex{}
	eventsFile, _ := extract.ExtractFile("./etl-test-files/EFMEvents.log")
	err := transform.TransformEventsFile(eventsFile)
	if err != nil {
		log.Panic(err)
	}
	efmFilesPath, err := utils.ReadDirectory("./etl-test-files", "efm")
	if err != nil {
		log.Panic("Could not read directory")
	}
	efLoadChan := make(chan string, len(efmFilesPath))
	efTransformChan := make(chan []*transform.TransformedElectricFieldLine, 1)
	for i := 0; i < workers; i++ {
		go worker(efLoadChan, efTransformChan, mux)
	}
	for _, filePath := range efmFilesPath {
		efLoadChan <- filePath
	}
	close(efLoadChan)
	for i := 0; i < len(efmFilesPath); i++ {
		<-efTransformChan
	}
}
