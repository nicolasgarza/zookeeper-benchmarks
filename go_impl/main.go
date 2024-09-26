package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

func main() {
	add_and_delete()
	time.Sleep(time.Second)
}

func add_and_delete() {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, 60*time.Second)
	if err != nil {
		log.Fatalf("Unable to connect to Zookeeper: %v", err)
	}
	defer conn.Close()

	rootPath := "/benchmark"
	if exists, _, err := conn.Exists(rootPath); !exists && err == nil {
		_, err = conn.Create(rootPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			log.Fatalf("Unable to create root znode: %v", err)
		}
	}

	// create a done channel
	done := make(chan struct{})

	znodeUpdater := func() {
		for {
			select {
			// check if we've received a stop signal
			case <-done:
				return // exit the goroutine
			default:
				// create a sequential znode
				conn.CreateProtectedEphemeralSequential(rootPath+"/node_", []byte{}, zk.WorldACL(zk.PermAll))
				if err != nil {
					log.Printf("Failed to create znode: %v", err)
					return
				}
			}
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			znodeUpdater()
		}()
	}

	time.Sleep(60 * time.Second)

	fmt.Println("Stopping goroutines...")

	close(done)

	wg.Wait()

	children, _, err := conn.Children(rootPath)
	if err != nil {
		log.Fatalf("Unable to get children of root znode: %v", err)
	}
	fmt.Printf("Number of znodes in %s: %d\n", rootPath, len(children))
}
