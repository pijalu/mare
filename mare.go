// Package mare provides a simple in memory map reduce.
package mare //github.com/pijalu/mare

import (
	"log"
	"sync"
)

// MapOutput define the output of a map function
type MapOutput struct {
	// Key of this mapped element
	Key interface{}
	// Actual value of the mapped element
	Value interface{}
}

// Mare structure
type Mare struct {
	trace        bool
	mapInWorkers sync.WaitGroup
	mapInChan    chan interface{}

	mapWorkerCnt  int
	mapOutWorkers sync.WaitGroup
	mapOutChan    chan MapOutput
}

// MaRe inits a map/reduce struct for the in memory M/R
func MaRe() *Mare {
	return &Mare{}
}

// Log enable/disable logging of Map / Reduce
func (m *Mare) Log(trace bool) *Mare {
	m.trace = trace
	return m
}

// MapWorker allows to (re)define the number of go routines to use to run the map (default 1)
func (m *Mare) MapWorker(count int) *Mare {
	m.mapWorkerCnt = count
	return m
}

// Map defines the Map function to use
// Note that the map function can return multiple output for a single value
func (m *Mare) Map(mapFunc func(input interface{}) []MapOutput) *Mare {
	if m.mapWorkerCnt == 0 {
		m.mapWorkerCnt++
	}
	if m.mapOutChan != nil {
		panic("Map already in progress !")
	}

	// Start the map
	m.mapOutWorkers.Add(m.mapWorkerCnt)
	m.mapOutChan = make(chan MapOutput, m.mapWorkerCnt)
	for i := 0; i < m.mapWorkerCnt; i++ {
		go func() {
			defer m.mapOutWorkers.Done()
			for item := range m.mapInChan {
				for _, output := range mapFunc(item) {
					if m.trace {
						log.Printf("Emit %v with key %v", output.Key, output.Value)
					}
					m.mapOutChan <- output
				}
			}
		}()
	}

	// Wait for end of work and close
	go func() {
		m.mapOutWorkers.Wait()
		close(m.mapOutChan)
	}()

	return m
}

// Reduce takes a closure and use them tp reduce values
func (m *Mare) Reduce(reduceFunc func(a, b interface{}) interface{}) map[interface{}]interface{} {
	results := make(map[interface{}]interface{})

	for item := range m.mapOutChan {
		mapItem, present := results[item.Key]
		if present {
			if m.trace {
				log.Printf("Reducing %v with %v for key %v", mapItem, item.Value, item.Key)
			}
			results[item.Key] = reduceFunc(mapItem, item.Value)
		} else {
			if m.trace {
				log.Printf("Saving %v for key %v", item.Value, item.Key)
			}
			results[item.Key] = item.Value
		}
	}

	return results
}

// prepareInput will create input channel
func (m *Mare) prepareInput() {
	// Add worker to channel
	m.mapInWorkers.Add(1)
	// Create channel and defer close until all workers are done
	if m.mapInChan == nil {
		m.mapInChan = make(chan interface{}, m.mapWorkerCnt)
		go func() {
			m.mapInWorkers.Wait()
			close(m.mapInChan)
		}()
	}
}

// InSlice takes a slice of interface for M/R
func (m *Mare) InSlice(items []interface{}) *Mare {
	m.prepareInput()

	// Start processing
	go func() {
		defer m.mapInWorkers.Done()
		for _, item := range items {
			m.mapInChan <- item
		}
	}()

	// Return m
	return m
}

// InChannel takes a channel and use it for MR
func (m *Mare) InChannel(items chan interface{}) *Mare {
	m.prepareInput()
	go func() {
		defer m.mapInWorkers.Done()
		for item := range items {
			m.mapInChan <- item
		}
	}()
	// Return m
	return m
}
