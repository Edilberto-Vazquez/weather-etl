package transformefmfiles

import (
	"errors"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/usecases/extractfile"
)

type TransformedEventLine struct {
	dateTime  string
	lightning bool
	distance  uint8
}

func isThereLightning(str string) bool {
	return regexp.MustCompile(`Lightning Detected`).MatchString(str)
}

func getEventDateTime(str string) (string, error) {
	var date string = regexp.MustCompile(`\d\d/\d\d/\d\d\d\d`).FindString(str)
	var duration string = regexp.MustCompile(`\d\d:\d\d:\d\d`).FindString(str)
	dateTime, err := time.Parse(time.RFC3339, date[6:]+"-"+date[0:2]+"-"+date[3:5]+"T"+duration+"Z")
	if err != nil {
		return "", err
	}
	return dateTime.UTC().String(), nil
}

func getDistance(str string) (uint8, error) {
	match := strings.Split(regexp.MustCompile(`at\s\d\d\skm|at\s\d\skm`).FindString(str), " ")
	distance, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return uint8(distance), nil
}

func transformEventLines(lines []string, path string) (processedLines []*TransformedEventLine) {
	for _, str := range lines {
		log.Println(str)
		if lightning := isThereLightning(str); lightning {
			dateTime, err := getEventDateTime(str)
			if err != nil {
				log.Printf("[TRANSFORM_EVENTS] transformEventLines: Could not get date from this line: %s; error: %s\n", str, err.Error())
				continue
			}
			distance, err := getDistance(str)
			if err != nil {
				log.Printf("[TRANSFORM_EVENTS] transformEventLines: Could not get distance from this line: %s; error: %s\n", str, err.Error())
				continue
			}
			processedLines = append(processedLines, &TransformedEventLine{
				dateTime:  dateTime,
				lightning: lightning,
				distance:  distance,
			})
		}
	}
	return
}

func TransformEventsFile(extractedFile *extractfile.ExtractedFile) ([]*TransformedEventLine, error) {
	if len(extractedFile.GetLines()) == 0 {
		return nil, errors.New("[TRANSFORM_EVENTS] TransformEventsFile: the log file has no information to be transformed")
	}
	return transformEventLines(extractedFile.GetLines(), extractedFile.GetFileName()), nil
}
