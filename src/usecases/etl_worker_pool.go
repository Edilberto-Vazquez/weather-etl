package usecases

import (
	"context"
	"fmt"
	"sync"

	"github.com/Edilberto-Vazquez/weather-etl/src/models"
	"github.com/Edilberto-Vazquez/weather-etl/src/repository"
	"golang.org/x/sync/semaphore"
)

type ETLWorkerPool struct {
	semaphore   *semaphore.Weighted
	wg          sync.WaitGroup
	filesList   []string
	repo        repository.Repository
	newPipeline models.NewETLPipeline
}

type ETLWorkerPoolConfig func(etl *ETLWorkerPool)

func NewETLWorkerPoolConfig(workers int64, repo repository.Repository, files []string, pipeline models.NewETLPipeline) ETLWorkerPoolConfig {
	return func(etl *ETLWorkerPool) {
		etl.repo = repo
		etl.semaphore = semaphore.NewWeighted(workers)
		etl.newPipeline = pipeline
		etl.filesList = files
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
	return nil
}

func (etl *ETLWorkerPool) addJob(file string) {
	etl.wg.Add(1)
	go func() {
		ctx := context.Background()
		defer etl.wg.Done()
		err := etl.semaphore.Acquire(ctx, 1)
		if err != nil {
			fmt.Println("Error acquiring semaphore:", err)
			return
		}
		defer etl.semaphore.Release(1)

		pipeline := etl.newPipeline(file, etl.repo)
		pipeline.RunETL(ctx)
	}()
}

func (etl *ETLWorkerPool) wait() {
	etl.wg.Wait()
}

func (etl *ETLWorkerPool) Run() {
	for _, file := range etl.filesList {
		etl.addJob(file)
	}
	etl.wait()
}
