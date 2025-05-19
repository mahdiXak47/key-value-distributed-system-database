package main

import (
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/mahdiXak47/key-value-distributed-system-database/controller/cluster"
	"github.com/mahdiXak47/key-value-distributed-system-database/controller/handlers"
)

func main() {
	cl := cluster.NewCluster()

	go cl.StartHealthChecks(5 * time.Second) // check every 5 seconds

	tmpl := template.Must(template.ParseFiles("template/index.html")) // parsing the html file
	http.HandleFunc("/", handlers.IndexHandler(cl, tmpl))
	http.HandleFunc("/node/add", handlers.AddNodeHandler(cl))
	http.HandleFunc("/node/remove", handlers.RemoveNodeHandler(cl))
	http.HandleFunc("/partition/add", handlers.AddPartitionHandler(cl))
	http.HandleFunc("/partition/remove", handlers.RemovePartitionHandler(cl))
	http.HandleFunc("/partition/transfer", handlers.TransferPartitionHandler(cl))
	http.HandleFunc("/partition/change-leader", handlers.ChangeLeaderHandler(cl))
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("./static"))))
	log.Println("Starting controller server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
