package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/constants"
	"github.com/Edilberto-Vazquez/weather-services/src/drivers"
	"github.com/Edilberto-Vazquez/weather-services/src/usecases"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

func usage() {
	msg := fmt.Sprintf(`usage: %s [OPTIONS]%s ETL for EFM-100 Atmospheric Electric Field Monitor`, constants.AppName, constants.AppName)
	fmt.Println(msg)
	flag.PrintDefaults()
}

func runETL(workers int, eventsFile, efmFiles string) {
	start := time.Now()
	etl := usecases.NewETL(usecases.Config(workers, drivers.NewMongoDBConnection()))
	etl.GetEFMEventLogs(eventsFile)
	etl.GetEFMFilePaths(efmFiles)
	etl.Run()
	duration := time.Since(start)
	log.Println(duration)
}

func main() {
	workers := flag.Int("workers", constants.Workers, "The number of workers that are processing the information (Workers should be equal to or less than CPU cores for best performance)")
	eventsFile := flag.String("events-file", "", "Path of the file with the EFM event logs")
	efmFiles := flag.String("efm-files", "", "Path of the folder with the EFM files")
	flag.Usage = usage
	flag.Parse()
	if *eventsFile != "" && *efmFiles != "" {
		runETL(*workers, *eventsFile, *efmFiles)
	} else {
		log.Println("-events-file or -efm-files parameter not passed, loading configuration from etl.conf file")
		file, err := utils.OpenFile("./etl.conf")
		if err != nil {
			file.Close()
			log.Fatalf("Could not open the etl.conf file Error: %s", err.Error())
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			parameter := strings.Split(scanner.Text(), "=")
			log.Println(scanner.Text())
			if parameter[1] == "" {
				log.Fatalf("Could not load the parameter %s from etl.conf value not found.\nAdd a value to the parameter %s in etl.conf or pass the flag -%s.\n--help for see all options", parameter[0], parameter[0], parameter[0])
			}
			switch parameter[0] {
			case "workers":
				v, _ := strconv.Atoi(parameter[1])
				workers = &v
			case "events-file":
				eventsFile = &parameter[1]
			case "efm-files":
				efmFiles = &parameter[1]
			}
		}
		file.Close()
		runETL(*workers, *eventsFile, *efmFiles)
	}
}
