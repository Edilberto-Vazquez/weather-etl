package usecases

import (
	"bufio"
	"fmt"
	"log"
	"path"
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
	filePath     string
	dbRepository repository.Repository
}

func NewWeatherETLPipeline(filepath string, dbRepository repository.Repository) *WeatherETLPipeline {
	return &WeatherETLPipeline{
		filePath:     filepath,
		dbRepository: dbRepository,
	}
}

func GetWeatherETLPipeline() models.NewETLPipeline {
	return func(filePath string, repo repository.Repository) models.ETLPipeline {
		return &WeatherETLPipeline{
			filePath:     filePath,
			dbRepository: repo,
		}
	}
}

func transformWeatherField(str string) (number float64) {
	if len(str) == 0 {
		return 0
	}
	number, err := strconv.ParseFloat(strings.Replace(str, ",", ".", 1), 64)
	if err != nil {
		return 0
	}
	return
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

func (w *WeatherETLPipeline) Transform(records []string) (transformedRecords []interface{}, err error) {
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
			TempIn:   transformWeatherField(splitRecord[1]),
			Temp:     transformWeatherField(splitRecord[2]),
			Chill:    transformWeatherField(splitRecord[3]),
			DewIn:    transformWeatherField(splitRecord[4]),
			Dew:      transformWeatherField(splitRecord[5]),
			HeatIn:   transformWeatherField(splitRecord[6]),
			Heat:     transformWeatherField(splitRecord[7]),
			HumIn:    transformWeatherField(splitRecord[8]),
			Hum:      transformWeatherField(splitRecord[9]),
			WspdHi:   transformWeatherField(splitRecord[10]),
			WspdAvg:  transformWeatherField(splitRecord[11]),
			WdirAvg:  transformWeatherField(splitRecord[12]),
			Bar:      transformWeatherField(splitRecord[13]),
			Rain:     transformWeatherField(splitRecord[14]),
			RainRate: transformWeatherField(splitRecord[15]),
		})
	}
	if len(transformedRecords) == 0 {
		return nil, fmt.Errorf("no lines could be transformed from file: %s;", path.Base(w.filePath))
	}
	return
}

func (w *WeatherETLPipeline) Load(records []interface{}) error {
	return w.dbRepository.InsertWeatherRecords(records)
}

func (w *WeatherETLPipeline) RunETL() error {
	log.Printf("[WEATHER_ETL] extracting %s", w.filePath)
	extractedRecords, err := w.Extract()
	if err != nil {
		log.Printf("[WEATHER_ETL] failed extracting %s; error: %s", w.filePath, err)
		return err
	}
	log.Printf("[WEATHER_ETL] transforming %s", w.filePath)
	transformedRecords, err := w.Transform(extractedRecords)
	if err != nil {
		log.Printf("[WEATHER_ETL] failed transfroming %s; error: %s", w.filePath, err)
		return err
	}
	log.Printf("[WEATHER_ETL] loading %s", w.filePath)
	err = w.Load(transformedRecords)
	if err != nil {
		log.Printf("[WEATHER_ETL] failed loading %s", w.filePath)
		return err
	}
	return nil
}
