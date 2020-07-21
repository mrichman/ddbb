package main

import (
	"flag"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
)

// var logger *zap.Logger
var sugar *zap.SugaredLogger

// worker performs the actual work against the table
func worker(id int, wg *sync.WaitGroup, tableName string) {

	defer wg.Done()

	sugar.Infof("Worker %d starting", id)

	for {
		err := batchWriteItems(tableName)
		if err != nil {
			sugar.Errorf("%v", err)
			break
		}
		sugar.Debug(".")
	}

}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func main() {

	// logger = zap.NewExample()
	// logger, _ := zap.NewDevelopment()
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar = logger.Sugar()

	var numWorkers int
	var tableName string
	var duration int
	flag.IntVar(&numWorkers, "w", 4, "Number of workers")
	flag.StringVar(&tableName, "t", "", "Table name")
	flag.IntVar(&duration, "d", 60, "Duration in seconds")
	flag.Parse()

	if len(tableName) == 0 {
		sugar.Fatal("Table name is required")
		flag.PrintDefaults()
		os.Exit(1)
	}

	err := discoverSchema(tableName)
	if err != nil {
		sugar.Fatalf("Error discovering DynamoDB table schema: %v", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, &wg, tableName)
	}

	timeout := time.Duration(duration) * time.Second
	sugar.Infof("Wait for waitgroup (up to %s)", timeout)

	if waitTimeout(&wg, timeout) {
		sugar.Infof("Timeout elapsed after %s", timeout)
	} else {
		sugar.Infof("Done")
	}
	sugar.Infof("Done")
}
