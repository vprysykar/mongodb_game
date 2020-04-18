package main

import (
	"flag"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"net/http"
	"time"
)

var (
	MongoCtx *mongo.Client
	DBNAME = "Gamepoint1"
)

func main() {
	connectionString := "mongodb://localhost:27017"
	var dir string
	var port string

	flag.StringVar(&dir, "dir", ".", "the directory to serve files from. Defaults to the current dir")
	flag.StringVar(&port, "port", "202", "network port. Default 80")
	flag.Parse()
	r := mux.NewRouter()

	// This will serve files under http://localhost:8000/static/<filename>
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir(dir))))
	r.HandleFunc("/api/users", handlerGetUsers).Methods(http.MethodGet)
	r.HandleFunc("/api/users/add", handlerUsersAdd).Methods(http.MethodPost)

	r.HandleFunc("/api/games", handlerGetGames).Methods(http.MethodGet)
	r.HandleFunc("/api/game/stats", handlerStatsGame).Methods(http.MethodGet)
	r.HandleFunc("/api/user/rank", handlerUserRanking).Methods(http.MethodGet)

	srv := &http.Server{
		Handler:      r,
		Addr:         "127.0.0.1:" + port,
		WriteTimeout: 45 * time.Second,
		ReadTimeout:  45 * time.Second,

	}

	mmongoClient, err := ConnectMongo(connectionString)
	if err != nil {
		log.Fatal(err)
	}
	MongoCtx = mmongoClient

	log.Println("starting server")
	log.Fatal(srv.ListenAndServe())
}
