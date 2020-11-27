package jobsprocessor

import (
	"hash"
	"hash/fnv"
	"log"
	"sync"

	"github.com/pkg/errors"
)

// Options for creating Processor
type Options struct {
	WorkersCount         int
	WorkerJobsBufferSize int
}

// Processor - main object for processing data
type Processor struct {
	workersCount         int
	workerJobsBufferSize int

	jobsChan chan *job
	workers  []*worker

	hasher hash.Hash32
	sync.Mutex
}

type job struct {
	syncKey  SyncKey
	data     interface{}
	callback Callback
}

type worker struct {
	jobs chan *job
}

// SyncKey - all jobs with this key will be processing synchronously
type SyncKey = string

// Callback for processing data
type Callback func(data interface{})

// Create Processor
func Create(options Options) *Processor {
	p := &Processor{
		jobsChan:             make(chan *job),
		workerJobsBufferSize: options.WorkerJobsBufferSize,
		workersCount:         options.WorkersCount,
		hasher:               fnv.New32(),
		workers:              make([]*worker, options.WorkersCount),
	}
	for i := range p.workers {
		p.workers[i] = &worker{
			jobs: make(chan *job, p.workerJobsBufferSize),
		}

		go p.workers[i].work()
	}

	go p.start()

	return p
}

// Process - synchronously processes data for one syncKey, asynchronously for different syncKeys.
func (p *Processor) Process(syncKey SyncKey, data interface{}, callback Callback) {
	p.jobsChan <- &job{
		syncKey:  syncKey,
		data:     data,
		callback: callback,
	}
}

func (p *Processor) start() {
	for j := range p.jobsChan {
		p.assignWorker(j)
	}
}

func (p *Processor) assignWorker(j *job) {
	workerID := p.getWorkerIDForSyncKey(j.syncKey)
	assignedWorker := p.workers[workerID]
	assignedWorker.addJob(j)
}

func (p *Processor) getWorkerIDForSyncKey(key SyncKey) uint32 {
	p.Lock()
	defer p.Unlock()

	p.hasher.Reset()
	_, err := p.hasher.Write([]byte(key))
	if err != nil {
		log.Fatal(errors.Wrap(err, "jobsprocessor getWorkerIdForSyncKey error #1"))
	}

	return p.hasher.Sum32() % uint32(len(p.workers))
}

func (w *worker) addJob(j *job) {
	w.jobs <- j
}

func (w *worker) work() {
	for j := range w.jobs {
		j.callback(j.data)
	}
}
