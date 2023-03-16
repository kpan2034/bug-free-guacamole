package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/exp/mmap"
)

type Reader interface {
	At(offset int) byte
	Close() error
	Len() int
}

type Task struct {
	// Some id
	id int
	// starting offset
	offset int
	// size of chunk
	size int
	// struct that implements Reader interface
	reader Reader
	// function to run
}

type output struct {
	// output id
	id int
	// pointer to the output bytes
	data *[]byte
}

// Compress function
func (t *Task) Do() *output {
	time.Sleep(50 * time.Millisecond)
	return compress(t)
}

func compress(t *Task) *output {
	data := make([]byte, 0)
	var count byte = 1
	for i := 1; i < t.size; i++ {
		if t.reader.At(i) == t.reader.At(i-1) {
			count = count + 1
		} else {
			data = append(data, t.reader.At(i-1), count)
			//fmt.Printf("%c%c", data[len(data)-2], data[len(data)-1])
			count = 1
		}
	}
	res := &output{t.id, &data}
	return res
}

type Pool struct {
	// number of workers
	n int

	// queue size
	b int

	// task queue
	queue chan *Task

	// results queue
	result chan *output

	// waitgroup to sync worker threads
	wg *sync.WaitGroup
}

// Return an initialized pool with n threads and b queue size
func NewPool(n, b int) (*Pool, error) {
	queue := make(chan *Task, b)
	result := make(chan *output, b)
	wg := &sync.WaitGroup{}
	return &Pool{n, b, queue, result, wg}, nil
}

func (p *Pool) AddTask(task *Task) {
	p.queue <- task
}

func (p *Pool) Wait() {
	close(p.queue)
	p.wg.Wait()
	close(p.result)
}

// Start the workers threads in the pool
func (p *Pool) Start() {
	for i := 0; i < p.n; i++ {
		// Increment waitgroup
		p.wg.Add(1)

		// Each worker waits for a task to arrive in the queue
		// which it picks up and then excutes
		//
		// Start worker goroutine
		go func(id int) {
			// When goroutine ends, decrease waitgroup counter
			defer p.wg.Done()

			// Wait for task to arrive
			for t := range p.queue {
				// Execute the task
				//fmt.Printf("%d\t%d\t%d\t%d\n", id, t.id, t.offset, t.size)
				p.result <- t.Do()
			}
		}(i)
	}
}

const MAX_CHUNK_SIZE = 4096

func main() {

	// j := flag.Int("j", 1, "number of goroutines to be used.")
	flag.Parse()

	// fmt.Printf("j:%d\n", *j)

	// Initialize threadpool
	pool, err := NewPool(1, 100)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	// Start the threadpool
	pool.Start()

	// Start a thread to collect the results

	wg := &sync.WaitGroup{}
	// map of all output data
	// this lets us order it w/o synchronization
	result := make(map[int]*[]byte)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range pool.result {
			result[r.id] = r.data
		}
	}()

	// Keeps track of the readers for each file so you can close them later
	var files []Reader

	id := 0
	// Iterate over all files
	for _, filename := range flag.Args() {
		// Open the file using mmap
		f, err := mmap.Open(filename)
		if err != nil {
			fmt.Println(err)
			continue
		}
		// Add to list of file readers
		files = append(files, f)

		// Total length of of underlying file
		len := f.Len()

		// Figure out # of tasks for this file
		numTasks := len / MAX_CHUNK_SIZE

		for i := 0; i < numTasks; i++ {
			// Create task
			task := &Task{
				id:     id,
				offset: i * MAX_CHUNK_SIZE,
				size:   MAX_CHUNK_SIZE,
				reader: f,
			}

			// Add task to threadpool queue
			pool.AddTask(task)

			// increament id
			id += 1
		}

		// Create final task if needed
		if size := len % MAX_CHUNK_SIZE; size > 0 {
			task := &Task{
				id:     id,
				offset: numTasks * MAX_CHUNK_SIZE,
				size:   size,
				reader: f,
			}
			id += 1
			pool.AddTask(task)
		}
	}

	// Wait for all tasks to be processed
	pool.Wait()

	// Wait for output collector to return
	wg.Wait()

	// Print data in order
	for _, v := range result {
		for i := 0; i < len(*v); i += 2 {
			fmt.Printf("%c%c", (*v)[i], (*v)[i+1])
		}
	}

	// Close open files
	for _, f := range files {
		f.Close()
	}
}
