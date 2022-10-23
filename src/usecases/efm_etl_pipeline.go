package usecases

import (
	"bufio"
	"fmt"
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
	filePath     string
	dbRepository repository.Repository
}

var (
	efmLogEvents    map[string]EFMLogEvent = make(map[string]EFMLogEvent, 0)
	dateTimeRegexp  *regexp.Regexp         = regexp.MustCompile(`\d\d/\d\d/\d\d\d\d\s\d\d:\d\d:\d\d`)
	lightningRegexp *regexp.Regexp         = regexp.MustCompile(`Lightning Detected`)
	distanceRegexp  *regexp.Regexp         = regexp.MustCompile(`at\s\d\d\skm|at\s\d\skm`)
)

func LoadEFMEventLogs(filePath string) error {
	var linesNotExtracted int
	var lightningsNotFound int
	var failExtractDateTime int
	var failExtractDistance int
	log.Printf("[EFM_ETL] loading efm events from: %s", filePath)
	file, err := utils.OpenFile(filePath)
	if err != nil {
		log.Fatalf("[EFM_ETL] could not open EFM event logs file from %s; error: %s", filePath, err)
	}
	scanner := bufio.NewScanner(file)
	log.Printf("[EFM_ETL] extracting event logs from: %s", filePath)
	for scanner.Scan() {
		s := scanner.Text()
		if !lightningRegexp.MatchString(s) {
			lightningsNotFound++
			linesNotExtracted++
			continue
		}
		match, err := utils.FindString(s, dateTimeRegexp)
		if err != nil {
			failExtractDateTime++
			linesNotExtracted++
			continue
		}
		dateTime, err := time.Parse(time.RFC3339, match[6:10]+"-"+match[0:2]+"-"+match[3:5]+"T"+match[11:]+"Z")
		if err != nil {
			failExtractDateTime++
			linesNotExtracted++
			continue
		}
		match, err = utils.FindString(s, distanceRegexp)
		if err != nil {
			failExtractDistance++
			linesNotExtracted++
			continue
		}
		var splitDistance []string = strings.Split(match, " ")
		distance, err := strconv.ParseInt(splitDistance[1], 10, 64)
		if err != nil {
			failExtractDistance++
			linesNotExtracted++
			continue
		}
		efmLogEvents[dateTime.UTC().String()] = EFMLogEvent{
			DateTime:  dateTime.UTC().String(),
			Lightning: true,
			Distance:  uint8(distance),
		}
	}
	if linesNotExtracted > 0 {
		log.Printf("[EFM_ETL] %d lines could not be extracted from: %s", linesNotExtracted, filePath)
		log.Printf("[EFM_ETL] %d do not contain lightning", lightningsNotFound)
		log.Printf("[EFM_ETL] %d failed to extract datetime", failExtractDateTime)
		log.Printf("[EFM_ETL] %d failed to extract distance", failExtractDistance)
	}
	log.Printf("[EFM_ETL] event logs extracted from: %s", filePath)
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

func NewEFMETLPipeline(file string, dbRepository repository.Repository) *EFMETLPipeline {
	return &EFMETLPipeline{
		filePath:     file,
		dbRepository: dbRepository,
	}
}

func GetEFMETLPipeline() models.NewETLPipeline {
	return func(filePath string, dbRepository repository.Repository) models.ETLPipeline {
		return &EFMETLPipeline{
			filePath:     filePath,
			dbRepository: dbRepository,
		}
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

func (efm *EFMETLPipeline) Transform(records []string) (transformedRecords []interface{}, err error) {
	calcAvg := groupLinesByTimeAndCalcAvg()
	for _, record := range records {
		splitStr := strings.Split(record, ",")
		if len(splitStr) != 3 {
			continue
		}
		date := strings.Split(path.Base(efm.filePath), "-")[1]
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
	if len(transformedRecords) == 0 {
		return nil, fmt.Errorf("no lines could be transformed from file: %s;", path.Base(efm.filePath))
	}
	return
}

func (efm *EFMETLPipeline) Load(records []interface{}) error {
	return efm.dbRepository.InsertEFMRecords(records)
}

func (efm *EFMETLPipeline) RunETL() error {
	log.Printf("[EFM_ETL] extracting %s", efm.filePath)
	extractedRecords, err := efm.Extract()
	if err != nil {
		log.Printf("[EFM_ETL] failed extracting %s; error: %s", efm.filePath, err)
		return err
	}
	log.Printf("[EFM_ETL] transforming %s", efm.filePath)
	transformedRecords, err := efm.Transform(extractedRecords)
	if err != nil {
		log.Printf("[EFM_ETL] failed transfroming %s; error: %s", efm.filePath, err)
		return err
	}
	log.Printf("[EFM_ETL] loading %s", efm.filePath)
	err = efm.Load(transformedRecords)
	if err != nil {
		log.Printf("[EFM_ETL] failed loading %s", efm.filePath)
		return err
	}
	return nil
}
