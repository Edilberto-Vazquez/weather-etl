package main

import (
	"fmt"

	"github.com/Edilberto-Vazquez/weather-services/fileprocessor/usecases"
)

func main() {
	// paths, _ := utils.ReadDirectory("/home/potatofy/Downloads/campo-electrico", "efm")
	// usecases.ProcessMultipleEfm(paths)

	logLines := usecases.ProcessLog("/home/potatofy/Downloads/campo-electrico/EFMEvents1.log")
	for _, v := range logLines {
		fmt.Println(v)
	}
}
