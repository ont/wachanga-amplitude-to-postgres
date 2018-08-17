package main

import (
	"log"
	"path/filepath"

	"gopkg.in/cheggaaa/pb.v1"
)

func QueueFiles(glob string) <-chan string {
	ch := make(chan string)

	files, err := filepath.Glob(glob)
	if err != nil {
		log.Fatal(err)
	}

	bar := pb.StartNew(len(files))
	go func() {
		for _, file := range files {
			ch <- file
			bar.Increment()
		}
		close(ch)
	}()

	return ch
}
