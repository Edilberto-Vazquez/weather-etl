package usecases

import (
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
	electricField float32
	rotorStatus   int64
}

func newEfDate(str, efTime string) string {
	var split []string = strings.Split(str, "-")
	var date string = split[1]
	timeStamp, _ := time.Parse(time.RFC3339, date[4:8]+"-"+date[0:2]+"-"+date[2:4]+"T"+efTime+"Z")
	return timeStamp.UTC().String()
}

func avgEfByDate(efSet []string) float32 {
	sum := 0.0
	divisor := float64(len(efSet))
	for _, value := range efSet {
		float, _ := strconv.ParseFloat(value, 32)
		sum += float
	}
	return float32((math.Round((sum / divisor * 100)) / 100))
}

func processEfByDateGroup() func(fileName, str string) (record []interface{}) {
	var date string
	var efAverageSet []string
	return func(fileName, str string) (record []interface{}) {
		var splitStr []string = strings.Split(str, ",")
		if efDate := newEfDate(fileName, splitStr[0]); date == "" || efDate == date {
			efAverageSet = append(efAverageSet, splitStr[1])
			date = efDate
			return nil
		} else {
			rotorStatus, _ := strconv.ParseInt(splitStr[2], 10, 8)
			record = []interface{}{efDate, avgEfByDate(efAverageSet), rotorStatus}
			date = efDate
			efAverageSet = []string{}
		}
		return
	}
}

func processEfLines(lines []string, fileName string) []efmRecord {
	processedLines := make([]efmRecord, 0)
	avgByDateGropu := processEfByDateGroup()
	for _, v := range lines {
		if record := avgByDateGropu(fileName, v); record != nil {
			processedLines = append(processedLines, efmRecord{
				dateTime:      record[0].(string),
				electricField: record[1].(float32),
				rotorStatus:   record[2].(int64),
			})
		}
	}
	return processedLines
}

func ProcessEfm(path string) (electricFields []efmRecord) {
	if lines, err := utils.ReadFile(path); err != nil {
		log.Fatal(err)
	} else {
		electricFields = processEfLines(lines, filepath.Base(path))
	}
	return
}
