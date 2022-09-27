package transformefmfiles

import (
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/usecases/extractfile"
)

type TransformedElectricFieldLine struct {
	dateTime      string
	electricField float64
	rotorStatus   bool
}

type efmValues struct {
	time          string
	electricField float64
	rotorStatus   bool
}

func getEfmValues(str string) (*efmValues, error) {
	var splitStr []string = strings.Split(str, ",")
	if len(splitStr) != 3 {
		return nil, fmt.Errorf("[TRANSFORM_ELECTRIC_FIELD] getEfmValues: not enough values to transform: %s", str)
	}
	time := splitStr[0]
	electricField, err := strconv.ParseFloat(splitStr[1], 64)
	if err != nil {
		return nil, err
	}
	rotorStatus := splitStr[2] == "0"
	return &efmValues{time, electricField, rotorStatus}, nil
}

func createEfDate(str, efTime string) (string, error) {
	var split []string = strings.Split(str, "-")
	var date string = split[1]
	timeStamp, err := time.Parse(time.RFC3339, date[4:8]+"-"+date[0:2]+"-"+date[2:4]+"T"+efTime+"Z")
	if err != nil {
		return "", err
	}
	return timeStamp.UTC().String(), nil
}

func groupLinesByTimeAndCalcAvg() func(time string, electricField float64) (avg float64) {
	var efTime string
	var sum, divisor float64
	return func(time string, electricField float64) (avg float64) {
		if efTime == "" || time == efTime {
			sum, divisor = sum+electricField, divisor+1
			efTime = time
			return 0
		} else {
			avg = math.Round((sum / divisor * 100)) / 100
			sum, divisor = 0, 0
			efTime = time
		}
		return
	}
}

func transformElectricFieldLines(lines []string, fileName string) (processedLines []*TransformedElectricFieldLine) {
	calcAvg := groupLinesByTimeAndCalcAvg()
	for _, str := range lines {
		efmValues, err := getEfmValues(str)
		if err != nil {
			log.Printf("[TRANSFORM_ELECTRIC_FIELD] transformElectricFieldLines: Could not process this line: %s; error: %s\n", str, err.Error())
			continue
		}
		dateTime, err := createEfDate(fileName, efmValues.time)
		if err != nil {
			log.Printf("[TRANSFORM_ELECTRIC_FIELD] transformElectricFieldLines: Could not process this line with this date: %s; error: %s\n", str, err.Error())
			continue
		}
		if avg := calcAvg(efmValues.time, efmValues.electricField); avg != 0 {
			processedLines = append(processedLines, &TransformedElectricFieldLine{
				dateTime:      dateTime,
				electricField: avg,
				rotorStatus:   efmValues.rotorStatus,
			})
		}
	}
	return
}

func TransformElectricFieldFile(extractedFile *extractfile.ExtractedFile) ([]*TransformedElectricFieldLine, error) {
	if len(extractedFile.GetLines()) == 0 {
		return nil, errors.New("[TRANSFORM_ELECTRIC_FIELD] TransformElectricFieldFile: no information to transform")
	}
	return transformElectricFieldLines(extractedFile.GetLines(), extractedFile.GetFileName()), nil
}
