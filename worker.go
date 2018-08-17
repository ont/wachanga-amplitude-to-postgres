package main

type Worker struct {
	collector *Collector
	dumper    *Dumper
	files     <-chan string
}

func NewWorker(collector *Collector, files <-chan string, dumper *Dumper) *Worker {
	return &Worker{
		collector: collector,
		files:     files,
		dumper:    dumper,
	}
}

func (w *Worker) Run() {
	for file := range w.files {
		//log.Println("Parsing file ", file)

		parser := NewParser(file)
		go parser.Run()

		for {
			rec := parser.GetRecord()
			if rec == nil {
				break
			}

			if recReady := w.collector.Prepare(rec); recReady != nil {
				// record is never seen before, save it to database
				w.dumper.Save(recReady)
			}
		}
	}
}
