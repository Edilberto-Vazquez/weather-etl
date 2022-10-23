package usecases

import (
	"bufio"
	"log"
	"math"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/domains"
	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/repository"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

type EFMLogEvent struct {
	DateTime  string
	Lightning bool
	Distance  uint8
}

type EFMETLPipeline struct {
	filePath string
	repo     repository.Repository
}

var (
	efmLogEvents                   = make(map[string]EFMLogEvent, 0)
	dateTimeRegexp  *regexp.Regexp = regexp.MustCompile(`\d\d/\d\d/\d\d\d\d\s\d\d:\d\d:\d\d`)
	lightningRegexp *regexp.Regexp = regexp.MustCompile(`Lightning Detected`)
	distanceRegexp  *regexp.Regexp = regexp.MustCompile(`at\s\d\d\skm|at\s\d\skm`)
)

func NewEFMETLPipeline(file string, repo repository.Repository) *EFMETLPipeline {
	return &EFMETLPipeline{
		filePath: file,
		repo:     repo,
	}
}

func GetEFMETLPipeline() models.NewETLPipeline {
	return func(filePath string, repo repository.Repository) models.ETLPipeline {
		return &EFMETLPipeline{
			filePath: filePath,
			repo:     repo,
		}
	}
}

func LoadEFMEventLogs(filePath string) error {
	basePath := path.Base(filePath)
	file, err := utils.OpenFile(filePath)
	if err != nil {
		log.Fatalf("Could not open EFM event logs file from %s", basePath)
	}
	scanner := bufio.NewScanner(file)
	log.Printf("Extracting event logs from: %s", basePath)
	for scanner.Scan() {
		s := scanner.Text()
		if !lightningRegexp.MatchString(s) {
			continue
		}
		match, err := utils.FindString(s, dateTimeRegexp)
		if err != nil {
			continue
		}
		dateTime, err := time.Parse(time.RFC3339, match[6:10]+"-"+match[0:2]+"-"+match[3:5]+"T"+match[11:]+"Z")
		if err != nil {
			continue
		}
		match, err = utils.FindString(s, distanceRegexp)
		if err != nil {
			continue
		}
		var splitDistance []string = strings.Split(match, " ")
		distance, err := strconv.ParseInt(splitDistance[1], 10, 64)
		if err != nil {
			continue
		}
		efmLogEvents[dateTime.UTC().String()] = EFMLogEvent{
			DateTime:  dateTime.UTC().String(),
			Lightning: true,
			Distance:  uint8(distance),
		}
	}
	log.Printf("Event logs extracted from: %s", basePath)
	return file.Close()
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

func (efm *EFMETLPipeline) Extract() (extractedRecords []string, err error) {
	file, err := utils.OpenFile(efm.filePath)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		extractedRecords = append(extractedRecords, scanner.Text())
	}
	return extractedRecords, file.Close()
}

func (efm *EFMETLPipeline) Transform(records []string) (transformedRecords []interface{}) {
	calcAvg := groupLinesByTimeAndCalcAvg()
	for _, record := range records {
		var splitStr []string = strings.Split(record, ",")
		if len(splitStr) != 3 {
			continue
		}
		var date string = strings.Split(path.Base(efm.filePath), "-")[1]
		dateTime, err := time.Parse(time.RFC3339, date[4:8]+"-"+date[0:2]+"-"+date[2:4]+"T"+splitStr[0]+"Z")
		if err != nil {
			continue
		}
		electricField, err := strconv.ParseFloat(splitStr[1], 64)
		if err != nil {
			continue
		}
		if avg := calcAvg(splitStr[0], electricField); avg != 0 {
			value, exist := efmLogEvents[dateTime.String()]
			efmElectricField := domains.EFMElectricField{
				DateTime:      dateTime.UTC(),
				ElectricField: avg,
				RotorFail:     splitStr[2] == "1",
			}
			if exist {
				efmElectricField.Lightning = value.Lightning
				efmElectricField.Distance = value.Distance
			} else {
				efmElectricField.Lightning = false
				efmElectricField.Distance = 0
			}
			transformedRecords = append(transformedRecords, efmElectricField)
		}
	}
	return
}

func (efm *EFMETLPipeline) Load(records []interface{}) error {
	return efm.repo.InsertEFMRecords(records)
}

func (efm *EFMETLPipeline) RunETL() error {
	extractedRecords, err := efm.Extract()
	if err != nil {
		return err
	}
	transformedRecords := efm.Transform(extractedRecords)
	err = efm.Load(transformedRecords)
	if err != nil {
		return err
	}
	return nil
}
