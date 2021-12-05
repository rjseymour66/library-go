package main

import (
	"log"

	"github.com/rjseymour66/library-go/config"
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
}
