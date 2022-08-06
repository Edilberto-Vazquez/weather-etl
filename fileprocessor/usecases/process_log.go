package usecases

import (
	"log"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/fileprocessor/utils"
)

type logRecord struct {
	dateTime  string
	lightning bool
	distance  uint8
}

func thereIsLightning(str string) bool {
	return regexp.MustCompile(`Lightning Detected`).MatchString(str)
}

func newLogDate(str string) string {
	var date string = regexp.MustCompile(`\d\d/\d\d/\d\d\d\d`).FindString(str)
	var duration string = regexp.MustCompile(`\d\d:\d\d:\d\d`).FindString(str)
	timeStamp, _ := time.Parse(time.RFC3339, date[6:]+"-"+date[0:2]+"-"+date[3:5]+"T"+duration+"Z")
	return timeStamp.UTC().String()
}

func newDistance(str string) uint8 {
	match := strings.Split(regexp.MustCompile(`at\s\d\d\skm|at\s\d\skm`).FindString(str), " ")
	distance, _ := strconv.ParseInt(match[1], 10, 64)
	return uint8(distance)
}

func processLogLines(lines []string, path string) (processedLines []logRecord) {
	for _, str := range lines {
		if lightning := thereIsLightning(str); lightning {
			processedLines = append(processedLines, logRecord{
				dateTime:  newLogDate(str),
				lightning: lightning,
				distance:  newDistance(str),
			})
		}
	}
	return processedLines
}

func ProcessLog(path string) (electricFields []logRecord) {
	if lines, err := utils.ReadFile(path); err != nil {
		log.Fatal(err)
	} else {
		if len(lines) > 0 {
			electricFields = processLogLines(lines, filepath.Base(path))
		}
	}
	return
}
