package extract

import (
	"bufio"
	"errors"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/models"
)

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

func EFMElectricFieldsExtraction(filPath string) (electricFields models.EFMElectricFields, err error) {
	file, err := os.Open(filPath)
	if err != nil {
		return nil, err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if fileInfo.Size() == 0 {
		return nil, errors.New("[ELECTRIC_FIELDS_EXTRACTION] no information to transform")
	}
	scanner := bufio.NewScanner(file)
	calcAvg := groupLinesByTimeAndCalcAvg()
	for scanner.Scan() {
		s := scanner.Text()
		var splitStr []string = strings.Split(s, ",")
		if len(splitStr) != 3 {
			log.Printf("[ELECTRIC_FIELDS_EXTRACTION] Not enough values to transform: %s\n", s)
			continue
		}
		var timeValue string = splitStr[0]
		electricField, err := strconv.ParseFloat(splitStr[1], 64)
		if err != nil {
			log.Printf("[ELECTRIC_FIELDS_EXTRACTION] Could not get the electric field from this line: %s; error: %s\n", s, err.Error())
			continue
		}
		dateTime, err := createEfDate(fileInfo.Name(), timeValue)
		if err != nil {
			log.Printf("[ELECTRIC_FIELDS_EXTRACTION] Could not process this line with this date: %s; error: %s\n", s, err.Error())
			continue
		}
		if avg := calcAvg(timeValue, electricField); avg != 0 {
			electricFields = append(electricFields, &models.EFMElectricField{
				DateTime:      dateTime,
				ElectricField: avg,
				RotorFail:     splitStr[2] == "1",
			})
		}
	}
	return electricFields, file.Close()
}
