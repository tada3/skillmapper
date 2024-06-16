package main

import (
	"log"
	"os"

	// Blank-import the function package so the init() runs
  	_ "github.com/tada3/skillmapper/tag-updater"
	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"
)

func main() {
	port := "8888"
	if envPort := os.Getenv("PORT"); envPort != "" {
		port = envPort
	}
	if err := funcframework.Start(port); err != nil {
		log.Fatalf("functionframework.Start: %v\n", err)
	}
}