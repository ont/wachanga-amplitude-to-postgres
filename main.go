package main

import (
	"log"
	"sync"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	glob     = kingpin.Arg("glob", "Files to parse").Required().String()
	workers  = kingpin.Flag("workers", "Number of workers").Short('w').Default("2").Int()
	host     = kingpin.Flag("host", "Postgres hostname").Required().String()
	user     = kingpin.Flag("user", "Postgres username").Default("postgres").String()
	password = kingpin.Flag("password", "Postgres password").String()
	db       = kingpin.Flag("db", "Postgres database").Required().String()
)

func main() {
	kingpin.Parse()

	collector := NewCollector()

	dumper := NewDumper(*host, *db, *user, *password)
	go dumper.Run()
	defer dumper.Stop()

	files := QueueFiles(*glob)

	var wg sync.WaitGroup
	wg.Add(*workers)
	for i := 0; i < *workers; i++ {
		go func() {
			NewWorker(collector, files, dumper).Run()
			wg.Done()
		}()
	}
	wg.Wait()

	// save records with some empty fields (locale, country or city)
	log.Println("saving partial records...")
	collector.ForEachUnsaved(func(rec *Record) {
		dumper.Save(rec)
	})
	log.Println("done!")
}
