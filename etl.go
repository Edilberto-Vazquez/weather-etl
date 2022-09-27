package main

import (
	"log"

	"github.com/Edilberto-Vazquez/weather-services/src/usecases/extractfile"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases/transformefmfiles"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

func EfmEtl(filePath string) ([]*transformefmfiles.TransformedElectricFieldLine, error) {
	log.Printf("[ELECTRIC_FIELD_ETL]: Extracting File %s", filePath)
	extFile, err := extractfile.ExtractFile(filePath)
	if err != nil {
		log.Printf("[ELECTRIC_FIELD_ETL] Error extracting file: %s; error: %v\n", filePath, err.Error())
	}
	tf, err := transformefmfiles.TransformElectricFieldFile(extFile)
	if err != nil {
		log.Printf("[ELECTRIC_FIELD_ETL] Error transforming file: %s; error: %v\n", filePath, err.Error())
		return nil, err
	}
	log.Printf("[ELECTRIC_FIELD_ETL] File %s processed successfully\n", filePath)
	return tf, nil
}

func worker(loadFile <-chan string, transformFile chan<- []*transformefmfiles.TransformedElectricFieldLine) {
	for file := range loadFile {
		tf, err := EfmEtl(file)
		if err != nil {
			continue
		}
		transformFile <- tf
	}
}

func main() {
	workers := 6
	// eventsFile, _ := extractfile.ExtractFile("./etl-test-files/EFMEvents.log")
	// transformefmfiles.TransformEventsFile(eventsFile)
	efmFilesPath, _ := utils.ReadDirectory("/home/potatofy/campo-electrico", "efm")
	efLoadChan := make(chan string, len(efmFilesPath))
	efTransformChan := make(chan []*transformefmfiles.TransformedElectricFieldLine, len(efmFilesPath))
	for i := 0; i < workers; i++ {
		go worker(efLoadChan, efTransformChan)
	}
	for _, filePath := range efmFilesPath {
		efLoadChan <- filePath
	}
	close(efLoadChan)
	for i := 0; i < len(efmFilesPath); i++ {
		<-efTransformChan
	}
}
