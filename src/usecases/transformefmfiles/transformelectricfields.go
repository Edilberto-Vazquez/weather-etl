package transformefmfiles

import (
	"errors"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/usecases/extractfile"
)

type TransformedElectricFieldLine struct {
	DateTime      string
	Lightning     bool
	ElectricField float64
	RotorStatus   bool
}

func createEfDate(str, electricFieldTime string) (string, error) {
	var split []string = strings.Split(str, "-")
	var date string = split[1]
	dateTime, err := time.Parse(time.RFC3339, date[4:8]+"-"+date[0:2]+"-"+date[2:4]+"T"+electricFieldTime+"Z")
	if err != nil {
		return "", err
	}
	return dateTime.UTC().String(), nil
}

func groupLinesByTimeAndCalcAvg() func(timeValue string, electricField float64) (avg float64) {
	var electricFieldTime string
	var sum, divisor float64
	return func(timeValue string, electricField float64) (avg float64) {
		if electricFieldTime == "" || timeValue == electricFieldTime {
			sum, divisor = sum+electricField, divisor+1
			electricFieldTime = timeValue
			return 0
		} else {
			avg = math.Round((sum / divisor * 100)) / 100
			sum, divisor = 0, 0
			electricFieldTime = timeValue
		}
		return avg
	}
}

func TransformElectricFieldFile(extractedFile *extractfile.ExtractedFile) (lines []*TransformedElectricFieldLine, err error) {
	if len(extractedFile.GetLines()) == 0 {
		return nil, errors.New("[TRANSFORM_ELECTRIC_FIELD] TransformElectricFieldFile: no information to transform")
	}
	calcAvg := groupLinesByTimeAndCalcAvg()
	var extractedLines []string = extractedFile.GetLines()
	for _, str := range extractedLines {
		var splitStr []string = strings.Split(str, ",")
		if len(splitStr) != 3 {
			log.Printf("[TRANSFORM_ELECTRIC_FIELD] transformElectricFieldLines: not enough values to transform: %s\n", str)
			continue
		}
		var timeValue string = splitStr[0]
		electricField, err := strconv.ParseFloat(splitStr[1], 64)
		if err != nil {
			log.Printf("[TRANSFORM_ELECTRIC_FIELD] transformElectricFieldLines: Could not get the electric field from this line: %s; error: %s\n", str, err.Error())
			continue
		}
		dateTime, err := createEfDate(extractedFile.GetFileName(), timeValue)
		if err != nil {
			log.Printf("[TRANSFORM_ELECTRIC_FIELD] transformElectricFieldLines: Could not process this line with this date: %s; error: %s\n", str, err.Error())
			continue
		}
		if avg := calcAvg(timeValue, electricField); avg != 0 {
			lines = append(lines, &TransformedElectricFieldLine{
				DateTime:      dateTime,
				Lightning:     false,
				ElectricField: avg,
				RotorStatus:   splitStr[2] == "0",
			})
		}
	}
	return lines, nil
}
