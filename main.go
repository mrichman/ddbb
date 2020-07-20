package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"
)

// worker performs the actual work against the table
func worker(id int, wg *sync.WaitGroup, tableName string) {

	defer wg.Done()

	fmt.Printf("Worker %d starting\n", id)

	for {
		err := batchWriteItems(tableName)
		if err != nil {
			fmt.Printf("%v\n", err)
			break
		}
		fmt.Printf(".")
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
	var numWorkers int
	var tableName string
	var duration int
	flag.IntVar(&numWorkers, "w", 4, "Number of workers")
	flag.StringVar(&tableName, "t", "", "Table name")
	flag.IntVar(&duration, "d", 60, "Duration in seconds")
	flag.Parse()

	if len(tableName) == 0 {
		fmt.Println("Table name is required")
		flag.PrintDefaults()
		os.Exit(1)
	}

	err := discoverSchema(tableName)
	if err != nil {
		fmt.Printf("Error discovering DynamoDB table schema: %v", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, &wg, tableName)
	}

	timeout := time.Duration(duration) * time.Second
	fmt.Printf("Wait for waitgroup (up to %s)\n", timeout)

	if waitTimeout(&wg, timeout) {
		fmt.Printf("\nTimeout elapsed after %s\n", timeout)
	} else {
		fmt.Println("Done")
	}
	fmt.Println("Done")
}
