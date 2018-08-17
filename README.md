## Amplitude to postgres parser
This is multithreaded custom parser for amplitude dump files.

Some aspects of parser is highly related to internal project of wachanga, therefore parser is not universal in general.
This code may be used as base for another custom amplitude parser.

## How to run
```
go build -o main && \
./main -w 5 --db=some-bd --password=some-password --host=localhost '/path/to/*/some/*.json.gz'
```
