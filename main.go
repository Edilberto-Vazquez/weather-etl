package main

import (
	"log"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/drivers"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases"
)

func main() {
	start := time.Now()
	etl := usecases.NewETL(usecases.Config(10, drivers.NewMongoDBConnection()))
	etl.GetEFMEventLogs("./etl-test-files/EFMEvents.log")
	etl.GetEFMFilePaths("/home/potatofy/campo-electrico")
	etl.Run()
	duration := time.Since(start)
	log.Println(duration)
}
