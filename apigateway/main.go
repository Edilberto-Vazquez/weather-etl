package main

import (
	"github.com/Edilberto-Vazquez/weather-services/fileprocessor/usecases"
	"github.com/Edilberto-Vazquez/weather-services/fileprocessor/utils"
)

func main() {
	paths, _ := utils.ReadDirectory("/mnt/d/DataSets/Conjuntos-originales/campo-electrico", "efm")
	usecases.ProcessMultipleEfm(paths)
}
