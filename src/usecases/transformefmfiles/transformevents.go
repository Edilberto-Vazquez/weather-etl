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

func matchString(str, regex string) (string, error) {
	var dateTimeRegex *regexp.Regexp = regexp.MustCompile(regex)
	if !dateTimeRegex.MatchString(str) {
		return "", errors.New("regular expression does not match")
	}
	return dateTimeRegex.FindString(str), nil
}

func getEventDateTime(str string) (string, error) {
	match, err := matchString(str, `\d\d/\d\d/\d\d\d\d\s\d\d:\d\d:\d\d`)
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
	match, err := matchString(str, `at\s\d\d\skm|at\s\d\skm`)
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

func TransformEventsFile(extractedFile *extractfile.ExtractedFile) (lines []*TransformedEventLine, err error) {
	if len(extractedFile.GetLines()) == 0 {
		return nil, errors.New("[TRANSFORM_EVENTS] TransformEventsFile: the log file has no information to be transformed")
	}
	for _, str := range extractedFile.GetLines() {
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
			lines = append(lines, &TransformedEventLine{
				dateTime:  dateTime,
				lightning: lightning,
				distance:  distance,
			})
		}
	}
	return
}
