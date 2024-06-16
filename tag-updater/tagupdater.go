package tagupdater

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"os"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"google.golang.org/api/iterator"
)

var (
	bigQueryClient *bigquery.Client
	storageClient  *storage.Client

	projectID = os.Getenv("PROJECT_ID")
	bucketName = os.Getenv("BUCKET_NAME")
	objectName = os.Getenv("OBJECT_NAME")

	ctxBg     = context.Background()
)

func init() {
	log.Printf("XXX init %v", 0)
	var err error
	bigQueryClient, err = bigquery.NewClient(ctxBg, projectID)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	storageClient, err = storage.NewClient(ctxBg)
	if err != nil {
		log.Fatalf("storage.NewClient: %v", err)
	}
	functions.HTTP("tag-updater", updateTags)
	log.Printf("XXX init %v", 999)
}

func updateTags(w http.ResponseWriter, r *http.Request) {
	log.Printf("XXX updateTags %v", 0)
	var err error
	numberOfTagsRetrieved, data, err := retrieveTags()
	if err != nil {
		log.Printf("failed to retrieve tags: %v\n", err)
		http.Error(w, "retrieving tags failed", http.StatusInternalServerError)
		return
	}
	err = writeFile(data)
	if err != nil {
		log.Printf("failed to write file: %v", err)
		http.Error(w, "writing file failed", http.StatusInternalServerError)
		return
	}
	message := fmt.Sprintf("%v tags retrieved and written to gs://%s/%s", numberOfTagsRetrieved, bucketName, objectName)
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, message)
	log.Printf("XXX updateTas %v", 999)
}

func retrieveTags() (numberOfTagsRetrieved int, data []byte, err error) {
	log.Printf("XXX retrieveTags %v", 0)
	var b bytes.Buffer
	q := bigQueryClient.Query(`
		SELECTX tag_name FROM bigquery-public-data.stackoverflow.tags
			ORDER BY tag_name limit 1`)
	it, err := q.Read(ctxBg)
	if err != nil {
		log.Printf("failed to execute query: %v", err)
		return
	}
	for {
		var row []bigquery.Value
		err = it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("failed to iterate through results: %v", err)
			return
		}
		b.WriteString(fmt.Sprintf("%v\n", row[0]))
		numberOfTagsRetrieved++
	}
	data = b.Bytes()
	log.Printf("XXX retrieveTags %v", 999)
	return numberOfTagsRetrieved, data, nil
}

func writeFile(data []byte) (err error) {
	log.Printf("XXX writeFile %v", 0)
	wc := storageClient.Bucket(bucketName).Object(objectName).NewWriter(ctxBg)
	_, err = wc.Write(data)
	if err != nil {
		log.Printf("failed to write: %v", err)
		return
	}
	err = wc.Close()
	if err != nil {
		log.Printf("failed to close: %v", err)
		return
	}
	log.Printf("XXX writeFile %v", 999)
	return nil
}