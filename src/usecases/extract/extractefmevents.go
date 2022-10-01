package extract

import (
	"bufio"
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

var (
	dateTimeRegexp  *regexp.Regexp = regexp.MustCompile(`\d\d/\d\d/\d\d\d\d\s\d\d:\d\d:\d\d`)
	lightningRegexp *regexp.Regexp = regexp.MustCompile(`Lightning Detected`)
	distanceRegexp  *regexp.Regexp = regexp.MustCompile(`at\s\d\d\skm|at\s\d\skm`)
)

func getEventDateTime(s string) (string, error) {
	match, err := utils.FindString(s, dateTimeRegexp)
	if err != nil {
		return "", err
	}
	dateTime, err := time.Parse(time.RFC3339, match[6:10]+"-"+match[0:2]+"-"+match[3:5]+"T"+match[11:]+"Z")
	if err != nil {
		return "", err
	}
	return dateTime.UTC().String(), nil
}

func getDistance(str string) (uint8, error) {
	match, err := utils.FindString(str, distanceRegexp)
	if err != nil {
		return 0, err
	}
	var splitDistance []string = strings.Split(match, " ")
	distance, err := strconv.ParseInt(splitDistance[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return uint8(distance), nil
}

func EFMEeventLogExtraction(filePath string, logEventsMap models.EFMLogEvents) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	if fileInfo.Size() == 0 {
		return errors.New("[EVENT_LOG_EXTRACTION] no information to transform")
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		s := scanner.Text()
		if !lightningRegexp.MatchString(s) {
			continue
		}
		dateTime, err := getEventDateTime(s)
		if err != nil {
			// log.Printf("[EVENT_LOG_EXTRACTION] Could not get date from: {%s}; Error: %s\n", s, err.Error())
			continue
		}
		distance, err := getDistance(s)
		if err != nil {
			// log.Printf("[EVENT_LOG_EXTRACTION] Could not get distance from: {%s}; Error: %s\n", s, err.Error())
			continue
		}
		logEventsMap[dateTime] = models.EFMLogEvent{
			DateTime:  dateTime,
			Lightning: true,
			Distance:  distance,
		}
	}
	return file.Close()
}
