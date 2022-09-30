package main

import (
	"log"

	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases/extract"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases/transform"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

var (
	EFMLogEvents = make(models.EFMLogEvents)
)

// func EfmEtl(filePath string, mux *sync.RWMutex) (transformedFile []*transform.TransformedElectricFieldLine, err error) {
// 	log.Printf("[ELECTRIC_FIELD_ETL] Extracting: %s", filePath)
// 	extFile, err := extract.ExtractFile(filePath)
// 	if err != nil {
// 		log.Printf("[ELECTRIC_FIELD_ETL] Error extracting: %s; error: %s\n", filePath, err.Error())
// 		return nil, err
// 	}
// 	transformedFile, err = transform.TransformElectricFieldFile(extFile, mux)
// 	if err != nil {
// 		log.Printf("[ELECTRIC_FIELD_ETL] Error transforming: %s; error: %s\n", filePath, err.Error())
// 		return nil, err
// 	}
// 	log.Printf("[ELECTRIC_FIELD_ETL] Processed successfully: %s\n", filePath)
// 	return transformedFile, nil
// }

func extractWorker(filePaths <-chan string, extractPipeline chan<- models.EFMElectricFields) {
	for path := range filePaths {
		log.Printf("Extracting: %s", path)
		electricFields, err := extract.EFMElectricFieldsExtraction(path)
		if err != nil {
			log.Printf("Error extracting: %s; error: %s\n", path, err.Error())
			continue
		}
		extractPipeline <- electricFields
	}
}

func transformWorker(extractPipeline <-chan models.EFMElectricFields, transformPipeline chan<- models.EFMTransformedLines) {
	for electricFields := range extractPipeline {
		log.Printf("Transforming")
		processedLines := transform.TransformEFMLines(EFMLogEvents, electricFields)
		transformPipeline <- processedLines
	}
}

func main() {
	workers := 8
	err := extract.EFMEeventLogExtraction("./etl-test-files/EFMEvents.log", EFMLogEvents)
	if err != nil {
		log.Panic(err)
	}
	efmFilesPath, err := utils.ReadDirectory("/home/potatofy/campo-electrico", "efm")
	// efmFilesPath, err := utils.ReadDirectory("./etl-test-files", "efm")
	if err != nil {
		log.Panic("Could not read directory")
	}
	filePaths := make(chan string, len(efmFilesPath))
	extractPipeline := make(chan models.EFMElectricFields, len(efmFilesPath))
	transformPipeline := make(chan models.EFMTransformedLines, len(efmFilesPath))
	for i := 0; i < workers; i++ {
		go extractWorker(filePaths, extractPipeline)
		go transformWorker(extractPipeline, transformPipeline)
	}
	for _, filePath := range efmFilesPath {
		filePaths <- filePath
	}
	close(filePaths)
	for i := 0; i < len(efmFilesPath); i++ {
		<-transformPipeline
	}
}
