package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/google/uuid"
)

type Parser struct {
	reader *bufio.Reader
	ch     chan *Record
}

func NewParser(filename string) *Parser {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	reader, err := gzip.NewReader(file)
	if err != nil {
		log.Fatal(err)
	}

	return &Parser{
		reader: bufio.NewReader(reader),
		ch:     make(chan *Record),
	}
}

func (p *Parser) GetRecord() *Record {
	return <-p.ch
}

func (p *Parser) ForRecord(callback func(*Record)) {
	for rec := range p.ch {
		callback(rec)
	}
}

func (p *Parser) Run() {
	defer close(p.ch)

	for {
		line, err := p.reader.ReadString('\n')

		if line != "" {
			if rec := p.parseLine(line); rec != nil {
				p.ch <- rec
			}
		}

		if err != nil && err != io.EOF {
			log.Println(err)
		}

		if err != nil {
			break
		}
	}
}

func (p *Parser) parseLine(line string) *Record {
	/*{
		  "city": "Helmbrechts",
		  "ip_address": "93.216.123.123",
		  "user_properties": {
			"Язык интерфейса": "de_de",
			"uuid": 1,
			"Фото": false,
			"Метрическая система": true,
			"Количество детей": "1",
			"Имя": "Some name",
			"Пол": "Мальчик",
			"Дата рождения": "Mon Oct 02 00:00:00 GMT+02:00 2017"
		  },
		  "location_lng": null,
		  "location_lat": null,

		  "event_properties": {
			"uuid": "37f63580-0e99-46e0-9e63-ff6ad4f8605e"
		  },

		  "language": "German",
		  "country": "Germany",
		  "region": "Bavaria",
	}*/

	var data struct {
		IpAddress      string     `json:"ip_address"`
		UserProperties Properties `json:"user_properties"`

		LocationLng float32 `json:"location_lng"`
		LocationLat float32 `json:"location_lat"`

		EventProperties Properties `json:"event_properties"`

		Language string `json:"language"`
		Country  string `json:"country"`
		Region   string `json:"region"`
		City     string `json:"city"`
	}

	err := json.Unmarshal([]byte(line), &data)
	if err != nil {
		log.Printf("%s '%s'", err, line)
		return nil
	}

	uuid := data.EventProperties.GetString("uuid")
	if uuid == "" || !p.isValidUUID(uuid) {
		return nil // drop records without valid UUID
	}

	return &Record{
		UUID:     uuid,
		Locale:   data.UserProperties.GetString("Язык интерфейса"),
		IsMetric: data.UserProperties.GetBool("Метрическая система"),

		Language: data.Language,

		Country: data.Country,
		Region:  data.Region,
		City:    data.City,

		IP: data.IpAddress,
	}
}

func (p *Parser) isValidUUID(str string) bool {
	_, err := uuid.Parse(str)
	return err == nil
}
