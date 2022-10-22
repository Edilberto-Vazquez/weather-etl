package usecases

import (
	"log"
	"path"
	"sync"

	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/repository"
)

type ETLWorkerPool struct {
	workers     int
	wg          sync.WaitGroup
	filesList   []string
	filesChan   chan string
	repository  repository.Repository
	newPipeline models.NewETLPipeline
}

type ETLWorkerPoolConfig func(etl *ETLWorkerPool)

func NewETLWorkerPoolConfig(workers int, repo repository.Repository, files []string, pipeline models.NewETLPipeline) ETLWorkerPoolConfig {
	return func(etl *ETLWorkerPool) {
		etl.repository = repo
		etl.workers = workers
		etl.newPipeline = pipeline
		etl.filesList = files
		etl.filesChan = make(chan string, len(files))
	}
}

func NewETLWorkerPool(cfgs ...ETLWorkerPoolConfig) *ETLWorkerPool {
	etl := new(ETLWorkerPool)
	for _, cfg := range cfgs {
		cfg(etl)
	}
	return etl
}

func (etl *ETLWorkerPool) SetPipeline(pipeline models.NewETLPipeline) {
	etl.newPipeline = pipeline
}

func (etl *ETLWorkerPool) SetFiles(files []string) error {
	etl.filesList = files
	etl.filesChan = make(chan string, len(files))
	return nil
}

func (etl *ETLWorkerPool) ETLWorker() {
	defer etl.wg.Done()
	for file := range etl.filesChan {
		pathBase := path.Base(file)
		pipeline := etl.newPipeline()
		log.Printf("Extracting: %s", pathBase)
		err := pipeline.Extract(file)
		if err != nil {
			log.Printf("Error extracting: %s; Error: %s\n", pathBase, err.Error())
			continue
		}
		log.Printf("Extracted: %s", pathBase)
		log.Printf("Transforming: %s", pathBase)
		pipeline.Transform()
		log.Printf("Transformed: %s", pathBase)
		log.Printf("Loading: %s", pathBase)
		err = pipeline.Load(etl.repository)
		if err != nil {
			log.Printf("Error loading: %s; Error: %s\n", pathBase, err.Error())
			continue
		}
		log.Printf("Loaded: %s", pathBase)
	}
}

func (etl *ETLWorkerPool) Run() {
	for i := 0; i < etl.workers; i++ {
		etl.wg.Add(1)
		go etl.ETLWorker()
	}
	for _, filePath := range etl.filesList {
		etl.filesChan <- filePath
	}
	close(etl.filesChan)
	etl.wg.Wait()
}
