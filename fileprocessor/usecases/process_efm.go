package usecases

import (
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/fileprocessor/utils"
)

type efmRecord struct {
	dateTime      string
	electricField float64
	rotorStatus   int64
}

func parseValues(str string) (efTime string, electricField float64, rotorStatus int64) {
	var splitStr []string = strings.Split(str, ",")
	efTime = splitStr[0]
	electricField, _ = strconv.ParseFloat(splitStr[1], 64)
	rotorStatus, _ = strconv.ParseInt(splitStr[2], 10, 8)
	return efTime, electricField, rotorStatus
}

func newEfDate(str, efTime string) string {
	var split []string = strings.Split(str, "-")
	var date string = split[1]
	timeStamp, _ := time.Parse(time.RFC3339, date[4:8]+"-"+date[0:2]+"-"+date[2:4]+"T"+efTime+"Z")
	return timeStamp.UTC().String()
}

func avgEf(sum, divisor float64) float64 {
	return math.Round((sum / divisor * 100)) / 100
}

func processEfByDateGroup() func(efTime string, electricField float64) (avg float64) {
	var date string
	var sum, divisor float64
	return func(efTime string, electricField float64) (avg float64) {
		if date == "" || efTime == date {
			sum, divisor = sum+electricField, divisor+1
			date = efTime
			return 0
		} else {
			avg = avgEf(sum, divisor)
			sum, divisor = 0.0, 0.0
			date = efTime
		}
		return avg
	}
}

func processEfLines(lines []string, fileName string) []efmRecord {
	processedLines := make([]efmRecord, 0)
	avgByDateGropu := processEfByDateGroup()
	for _, str := range lines {
		hour, electricField, rotorStatus := parseValues(str)
		if avg := avgByDateGropu(hour, electricField); avg != 0 {
			processedLines = append(processedLines, efmRecord{
				dateTime:      newEfDate(fileName, hour),
				electricField: avg,
				rotorStatus:   rotorStatus,
			})
		}
	}
	return processedLines
}

func ProcessEfm(path string) (electricFields []efmRecord) {
	if lines, err := utils.ReadFile(path); err != nil {
		log.Fatal(err)
	} else {
		if len(lines) > 0 {
			electricFields = processEfLines(lines, filepath.Base(path))
		}
	}
	return
}

func Worker(id int, jobs <-chan string, results chan<- []efmRecord) {
	for job := range jobs {
		processedLines := ProcessEfm(job)
		fmt.Println("file", job, "processed")
		results <- processedLines
	}
}

func ProcessMultipleEfm(paths []string) {
	nWorkers := 4
	jobs := make(chan string, len(paths))
	results := make(chan []efmRecord, len(paths))
	for i := 0; i < nWorkers; i++ {
		go Worker(i, jobs, results)
	}
	for _, efmFile := range paths {
		jobs <- efmFile
	}
	close(jobs)
	for i := 0; i < len(paths); i++ {
		<-results
	}
}
