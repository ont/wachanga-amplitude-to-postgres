package main

import (
	"fmt"
	"log"

	"github.com/davecgh/go-spew/spew"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type Dumper struct {
	db    *sqlx.DB
	ch    chan *Record
	tx    *sqlx.Tx
	bsize int // batch size
}

var schema = `
CREATE TABLE IF NOT EXISTS amplitude (
	id uuid PRIMARY KEY,

	locale varchar,
	language varchar,
	country varchar,
	region varchar,
	city varchar,

	ip inet,

	is_metric boolean
)
`

func NewDumper(host, dbname, user, password string) *Dumper {
	db, err := sqlx.Connect("postgres", fmt.Sprintf(
		"user=%s password=%s host=%s dbname=%s sslmode=disable",
		user, password, host, dbname,
	))

	if err != nil {
		log.Fatal(err)
	}

	db.MustExec(schema)

	return &Dumper{
		db:    db,
		ch:    make(chan *Record),
		bsize: 100, // TODO: move to kingpin
	}
}

func (d *Dumper) Save(rec *Record) {
	d.ch <- rec
}

func (d *Dumper) Run() {
	d.flush()

	cnt := 0
	for rec := range d.ch {
		cnt++

		d.addRec(rec)

		if cnt >= d.bsize {
			cnt = 0
			d.flush()
		}
	}
}

func (d *Dumper) flush() {
	if d.tx != nil {
		d.tx.Commit()
	}

	d.tx = d.db.MustBegin()
}

func (d *Dumper) addRec(rec *Record) {
	_, err := d.tx.NamedExec(`
		INSERT INTO amplitude(id, locale, language, country, region, city, ip, is_metric)
		VALUES (:uuid, :locale, :language, :country, :region, :city, :ip, :is_metric)
	`, rec)

	if err != nil {
		log.Fatal(err)
		spew.Dump(rec)
	}
}

func (d *Dumper) Stop() {
	close(d.ch)
	d.flush()
}
