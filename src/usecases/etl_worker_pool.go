package usecases

import (
	"context"
	"sync"

	"github.com/Edilberto-Vazquez/weather-services/src/models"
	"github.com/Edilberto-Vazquez/weather-services/src/repository"
)

type ETLWorkerPool struct {
	workers     int
	wg          sync.WaitGroup
	filesList   []string
	filesChan   chan string
	repo        repository.Repository
	newPipeline models.NewETLPipeline
}

type ETLWorkerPoolConfig func(etl *ETLWorkerPool)

func NewETLWorkerPoolConfig(workers int, repo repository.Repository, files []string, pipeline models.NewETLPipeline) ETLWorkerPoolConfig {
	return func(etl *ETLWorkerPool) {
		etl.repo = repo
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

func (etl *ETLWorkerPool) ETLWorker(ctx context.Context) {
	defer etl.wg.Done()
	for file := range etl.filesChan {
		pipeline := etl.newPipeline(file, etl.repo)
		pipeline.RunETL(ctx)
	}
}

func (etl *ETLWorkerPool) Run() {
	ctx := context.Background()
	for i := 0; i < etl.workers; i++ {
		etl.wg.Add(1)
		go etl.ETLWorker(ctx)
	}
	for _, filePath := range etl.filesList {
		etl.filesChan <- filePath
	}
	close(etl.filesChan)
	etl.wg.Wait()
}
