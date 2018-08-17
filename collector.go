package main

import (
	"sync"
)

type Collector struct {
	recs map[string]*Record // record for uuid (filled during multiple Prepare())

	lock sync.RWMutex
}

func NewCollector() *Collector {
	return &Collector{
		recs: make(map[string]*Record),
	}
}

func (c *Collector) Prepare(rec *Record) *Record {
	c.lock.RLock()
	stored, found := c.recs[rec.UUID]
	c.lock.RUnlock()

	if found {
		if stored.saved {
			return nil // record already processed
		}

		if *stored != *rec {
			rec = c.merge(stored, rec)
			c.save(rec)
		}
	} else {
		c.save(rec)
	}

	if c.isFull(rec) {
		rec.saved = true
		return rec
	}

	return nil
}

func (c *Collector) merge(first, second *Record) *Record {
	rec := *first

	// TODO: probably we need something better...
	if rec.City == "" {
		rec.City = second.City
	}
	if rec.Locale == "" {
		rec.Locale = second.Locale
	}
	if rec.Language == "" {
		rec.Language = second.Language
	}
	if rec.Country == "" {
		rec.Country = second.Country
	}
	if rec.Region == "" {
		rec.Region = second.Region
	}
	if rec.City == "" {
		rec.City = second.City
	}
	if rec.IP == "" {
		rec.IP = second.IP
	}

	return &rec
}

func (c *Collector) isFull(rec *Record) bool {
	return rec.Locale != "" && rec.Language != "" && rec.Country != ""
	/*
		return rec.City != "" && rec.Locale != "" && rec.Language != "" &&
			rec.Country != "" && rec.Region != "" && rec.City != "" && rec.IP != ""
	*/
}

func (c *Collector) save(rec *Record) {
	c.lock.Lock()
	c.recs[rec.UUID] = rec
	c.lock.Unlock()
}

func (c *Collector) ForEachUnsaved(callback func(rec *Record)) {
	for _, rec := range c.recs {
		if !rec.saved {
			callback(rec)
		}
	}
}
