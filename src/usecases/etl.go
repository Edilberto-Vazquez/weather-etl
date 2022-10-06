package usecases

import (
	"bufio"
	"log"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Edilberto-Vazquez/weather-services/src/constants"
	"github.com/Edilberto-Vazquez/weather-services/src/domains"
	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/repository"
	"github.com/Edilberto-Vazquez/weather-services/src/utils"
)

type ETL struct {
	transformedLines repository.EFMRepository
	efmLogEvents     models.EFMLogEvents
	efmFiles         []string
	workers          int
	wg               sync.WaitGroup
	efmFilesChan     chan string
}

type ETLConfig func(etl *ETL) error

func NewETL(cfgs ...ETLConfig) *ETL {
	etl := &ETL{}
	for _, cfg := range cfgs {
		err := cfg(etl)
		if err != nil {
			log.Panic("Could not initialize ETL")
		}
	}
	return etl
}

func Config(workers int, repo repository.EFMRepository) ETLConfig {
	return func(etl *ETL) error {
		etl.transformedLines = repo
		etl.workers = workers
		etl.efmLogEvents = make(models.EFMLogEvents)
		etl.efmFiles = make([]string, 0)
		return nil
	}
}

func (etl *ETL) GetEFMEventLogs(filePath string) {
	basePath := path.Base(filePath)
	file, err := utils.OpenFile(filePath)
	if err != nil {
		log.Panicf("Could not open EFM event logs file from %s", basePath)
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
		etl.efmLogEvents[dateTime.UTC().String()] = models.EFMLogEvent{
			DateTime:  dateTime.UTC().String(),
			Lightning: true,
			Distance:  uint8(distance),
		}
	}
	log.Printf("Event logs extracted from: %s", basePath)
}

func (etl *ETL) GetEFMFilePaths(filesPath string) {
	log.Printf("Loading paths from efm files: %s", filesPath)
	efmFiles, err := utils.ReadDirectory(filesPath, "efm")
	if err != nil {
		log.Panicf("Could not loading paths from: %s", filesPath)
	}
	etl.efmFiles = efmFiles
	etl.efmFilesChan = make(chan string, len(efmFiles))
}

func (etl *ETL) Run() {
	for i := 0; i < etl.workers; i++ {
		etl.wg.Add(1)
		go func() {
			defer etl.wg.Done()
			for file := range etl.efmFilesChan {
				pathBase := path.Base(file)
				efmPipeline := domains.NewEFMPipeline()
				log.Printf("Extracting: %s", pathBase)
				err := efmPipeline.Extract(file)
				if err != nil {
					log.Printf("Error extracting: %s; Error: %s\n", pathBase, err.Error())
					continue
				}
				log.Printf("Extracted: %s", pathBase)
				log.Printf("Transforming: %s", pathBase)
				efmPipeline.Transform(etl.efmLogEvents)
				log.Printf("Transformed: %s", pathBase)
				log.Printf("Loading: %s", pathBase)
				err = efmPipeline.Load(etl.transformedLines)
				if err != nil {
					log.Printf("Error loading: %s; Error: %s\n", pathBase, err.Error())
					continue
				}
				log.Printf("Loaded: %s", pathBase)
			}
		}()
	}
	for _, filePath := range etl.efmFiles {
		etl.efmFilesChan <- filePath
	}
	close(etl.efmFilesChan)
	etl.wg.Wait()
}
