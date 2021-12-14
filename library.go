package main

import (
	"log"
	"sync"

	"github.com/rjseymour66/library-go/config"
	"github.com/rjseymour66/library-go/server"
	_ "github.com/lib/pq"
)

func main() {
	log.Println("Starting Library Server.")

	// Main function code
	log.Println("Initializing configuration")
	err := config.InitConfig("library", nil)
	if err != nil {
		log.Fatalf("Failed to read configuration: %v\n", err)
	}

	log.Println("Library Server Stopped.")
	
	// db init
	log.Println("Initializing database")
	err = dbserver.InitializeDb()
	if err != {
		log.Fatalf("Could not access database: %v\n", err)
	}

	// Start the HTTP server
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		log.Println("Starting HTTP Server")

		err := server.StartHTTPServer()
		if err != nil {
			log.Fatalf("Could not start HTTP Server: %v\n", err)
		}
		log.Println("HTTP Server gracefully terminated.")
	}()

	wg.Wait()
}
