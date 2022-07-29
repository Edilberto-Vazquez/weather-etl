package main

import (
	"fmt"

	"github.com/Edilberto-Vazquez/weather-services/fileprocessor/usecases"
)

func main() {
	a := usecases.ProcessEfm("/mnt/c/Users/potat/Downloads/INAOE parque-01012017.efm")
	for _, v := range a {
		fmt.Println(v)
	}
}
