package usecases

import (
	"bufio"
	"strconv"
	"strings"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/domains"
	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/repository"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
	"golang.org/x/text/encoding/unicode"
)

type WeatherETLPipeline struct {
	filePath string
	repo     repository.Repository
}

func NewWeatherETLPipeline(filepath string, repository repository.Repository) *WeatherETLPipeline {
	return &WeatherETLPipeline{
		filePath: filepath,
		repo:     repository,
	}
}

func GetWeatherETLPipeline() models.NewETLPipeline {
	return func(filePath string, repo repository.Repository) models.ETLPipeline {
		return &WeatherETLPipeline{
			filePath: filePath,
			repo:     repo,
		}
	}
}

func DecodeField(str string) float64 {
	if len(str) == 0 {
		return 0
	}
	number, err := strconv.ParseFloat(strings.Replace(str, ",", ".", 1), 64)
	if err != nil {
		return 0
	}
	return number
}

func (w *WeatherETLPipeline) Extract() (extractedRecords []string, err error) {
	file, err := utils.OpenFile(w.filePath)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)
	decoder := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder()
	for scanner.Scan() {
		record, err := decoder.String(scanner.Text())
		if err != nil {
			continue
		}
		extractedRecords = append(extractedRecords, record)
	}
	return extractedRecords, file.Close()
}

func (w *WeatherETLPipeline) Transform(records []string) (transformedRecords []interface{}) {
	for i, record := range records {
		if i == 0 {
			continue
		}
		if i == len(records)-1 {
			continue
		}
		splitRecord := strings.Split(record, ";")
		splitDateTime := strings.Split(splitRecord[0], " ")
		dateTimeFormat := splitDateTime[0] + "T" + splitDateTime[1] + "Z"
		dateTime, err := time.Parse(time.RFC3339, dateTimeFormat)
		if err != nil {
			continue
		}
		transformedRecords = append(transformedRecords, domains.WeatherRecords{
			DateTime: dateTime,
			TempIn:   DecodeField(splitRecord[1]),
			Temp:     DecodeField(splitRecord[2]),
			Chill:    DecodeField(splitRecord[3]),
			DewIn:    DecodeField(splitRecord[4]),
			Dew:      DecodeField(splitRecord[5]),
			HeatIn:   DecodeField(splitRecord[6]),
			Heat:     DecodeField(splitRecord[7]),
			HumIn:    DecodeField(splitRecord[8]),
			Hum:      DecodeField(splitRecord[9]),
			WspdHi:   DecodeField(splitRecord[10]),
			WspdAvg:  DecodeField(splitRecord[11]),
			WdirAvg:  DecodeField(splitRecord[12]),
			Bar:      DecodeField(splitRecord[13]),
			Rain:     DecodeField(splitRecord[14]),
			RainRate: DecodeField(splitRecord[15]),
		})
	}
	return
}

func (w *WeatherETLPipeline) Load(records []interface{}) error {
	return w.repo.InsertWeatherRecords(records)
}

func (w *WeatherETLPipeline) RunETL() error {
	extractedRecords, err := w.Extract()
	if err != nil {
		return err
	}
	transformedRecords := w.Transform(extractedRecords)
	err = w.Load(transformedRecords)
	if err != nil {
		return err
	}
	return nil
}
