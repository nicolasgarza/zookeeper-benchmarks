package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	benchmarkPath = "/benchmark"
	numTasks      = 1000
	batchSize     = 100
)

func main() {
	ephemeralSequentialTest()
}

func ephemeralSequentialTest() {
	duration := 10 * time.Second
	startTime := time.Now()

	// Connect to ZooKeeper
	zkClient, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second)
	if err != nil {
		log.Fatalf("Failed to connect to ZooKeeper: %v", err)
	}
	defer zkClient.Close()

	// Check and delete the old benchmark node if it exists
	exists, stat, err := zkClient.Exists(benchmarkPath)
	if err != nil {
		log.Fatalf("Error checking if node exists: %v", err)
	}
	if exists {
		if err := zkClient.Delete(benchmarkPath, stat.Version); err != nil {
			log.Printf("Failed to delete old benchmark node: %v", err)
		}
	}

	// Create the benchmark node
	_, err = zkClient.Create(benchmarkPath, []byte{}, zk.FlagPersistent, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		log.Fatalf("Failed to create benchmark node: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(startTime) < duration {
				ops := make([]interface{}, 0, batchSize)
				for j := 0; j < batchSize; j++ {
					path := fmt.Sprintf("%s/node_", benchmarkPath)
					ops = append(ops, &zk.CreateRequest{
						Path:  path,
						Data:  []byte{},
						Acl:   zk.WorldACL(zk.PermAll),
						Flags: zk.FlagEphemeralSequential,
					})
				}

				_, err := zkClient.Multi(ops...)
				if err != nil {
					log.Printf("Error running multi request: %v", err)
				}
			}
		}()
	}

	// Wait for all tasks to complete
	wg.Wait()

	// Get the number of children
	children, _, err := zkClient.Children(benchmarkPath)
	if err != nil {
		log.Fatalf("Failed to get children: %v", err)
	}

	numChildren := len(children)
	fmt.Printf("Total znodes added: %d\n", numChildren)
	fmt.Printf("Operations per second: %.2f\n", float64(numChildren)/duration.Seconds())
}
