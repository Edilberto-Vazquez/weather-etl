package domains

import (
	"bufio"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/repository"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

type EFMElectricField struct {
	DateTime      time.Time `bson:"date_time"`
	Lightning     bool      `bson:"lightning"`
	ElectricField float64   `bson:"electric_field"`
	Distance      uint8     `bson:"distance"`
	RotorFail     bool      `bson:"rotor_fail"`
}

type EFMPipeline struct {
	ElectricFields []interface{}
}

func NewEFMPipeline() *EFMPipeline {
	return &EFMPipeline{ElectricFields: make([]interface{}, 0)}
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

func (efm *EFMPipeline) Extract(filePath string) (err error) {
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
			efm.ElectricFields = append(efm.ElectricFields, EFMElectricField{
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

func (efm *EFMPipeline) Transform(logEventsMap models.EFMLogEvents) {
	for i := 0; i < len(efm.ElectricFields); i++ {
		value, exist := logEventsMap[efm.ElectricFields[i].(EFMElectricField).DateTime.String()]
		if exist {
			electricField := efm.ElectricFields[i].(EFMElectricField)
			electricField.Lightning = value.Lightning
			electricField.Distance = value.Distance
			efm.ElectricFields[i] = electricField
		}
	}
}

func (efm *EFMPipeline) Load(repo repository.EFMRepository) error {
	return repo.InsertTransformedLines(efm.ElectricFields)
}
