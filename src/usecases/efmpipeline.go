package usecases

import (
	"bufio"
	"log"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/constants"
	"github.com/Edilberto-Vazquez/weather-services/src/domains"
	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/repository"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

var (
	efmLogEvents = make(models.EFMLogEvents)
)

type EFMETLPipeline struct {
	electricFields []interface{}
}

func NewEFMETLPipeline() *EFMETLPipeline {
	return &EFMETLPipeline{electricFields: make([]interface{}, 0)}
}

func GetEFMETLPipeline() models.NewETLPipeline {
	return func() models.ETLPipeline {
		return &EFMETLPipeline{electricFields: make([]interface{}, 0)}
	}
}

func SetEFMEventLogs(filePath string) {
	basePath := path.Base(filePath)
	file, err := utils.OpenFile(filePath)
	if err != nil {
		log.Fatalf("Could not open EFM event logs file from %s", basePath)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	log.Printf("Extracting event logs from: %s", basePath)
	for scanner.Scan() {
		s := scanner.Text()
		if !constants.LightningRegexp.MatchString(s) {
			continue
		}
		match, err := utils.FindString(s, constants.DateTimeRegexp)
		if err != nil {
			// log.Printf("[EVENT_LOG_EXTRACTION] Could not get date from: {%s}; Error: %s\n", s, err.Error())
			continue
		}
		dateTime, err := time.Parse(time.RFC3339, match[6:10]+"-"+match[0:2]+"-"+match[3:5]+"T"+match[11:]+"Z")
		if err != nil {
			// log.Printf("[EVENT_LOG_EXTRACTION] Could not parse date: {%s}; Error: %s\n", s, err.Error())
			continue
		}
		match, err = utils.FindString(s, constants.DistanceRegexp)
		if err != nil {
			// log.Printf("[EVENT_LOG_EXTRACTION] Could not get distance from: {%s}; Error: %s\n", s, err.Error())
			continue
		}
		var splitDistance []string = strings.Split(match, " ")
		distance, err := strconv.ParseInt(splitDistance[1], 10, 64)
		if err != nil {
			// log.Printf("[EVENT_LOG_EXTRACTION] Could not parse distance: {%s}; Error: %s\n", s, err.Error())
			continue
		}
		efmLogEvents[dateTime.UTC().String()] = models.EFMLogEvent{
			DateTime:  dateTime.UTC().String(),
			Lightning: true,
			Distance:  uint8(distance),
		}
	}
	log.Printf("Event logs extracted from: %s", basePath)
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

func (efm *EFMETLPipeline) Extract(filePath string) error {
	file, err := utils.OpenFile(filePath)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	calcAvg := groupLinesByTimeAndCalcAvg()
	for scanner.Scan() {
		s := scanner.Text()
		var splitStr []string = strings.Split(s, ",")
		if len(splitStr) != 3 {
			// log.Printf("[ELECTRIC_FIELDS_EXTRACTION] Not enough values to transform: {%s}\n", s)
			continue
		}
		var date string = strings.Split(path.Base(filePath), "-")[1]
		dateTime, err := time.Parse(time.RFC3339, date[4:8]+"-"+date[0:2]+"-"+date[2:4]+"T"+splitStr[0]+"Z")
		if err != nil {
			continue
		}
		if err != nil {
			// log.Printf("[ELECTRIC_FIELDS_EXTRACTION] Could not get date from: {%s}; Error: %s\n", s, err.Error())
			continue
		}
		electricField, err := strconv.ParseFloat(splitStr[1], 64)
		if err != nil {
			// log.Printf("[ELECTRIC_FIELDS_EXTRACTION] Could not get the electric field from: {%s}; Error: %s\n", s, err.Error())
			continue
		}
		if avg := calcAvg(splitStr[0], electricField); avg != 0 {
			efm.electricFields = append(efm.electricFields, domains.EFMElectricField{
				DateTime:      dateTime.UTC(),
				Lightning:     false,
				ElectricField: avg,
				Distance:      0,
				RotorFail:     splitStr[2] == "1",
			})
		}
	}
	return file.Close()
}

func (efm *EFMETLPipeline) Transform() {
	for i := 0; i < len(efm.electricFields); i++ {
		value, exist := efmLogEvents[efm.electricFields[i].(domains.EFMElectricField).DateTime.String()]
		if exist {
			electricField := efm.electricFields[i].(domains.EFMElectricField)
			electricField.Lightning = value.Lightning
			electricField.Distance = value.Distance
			efm.electricFields[i] = electricField
		}
	}
}

func (efm *EFMETLPipeline) Load(repo repository.Repository) error {
	return repo.InsertTransformedLines(efm.electricFields)
}
