package jobsprocessor

// Options for creating Processor
type Options struct {
	WorkersCount         int
	WorkerJobsBufferSize int
}

// Processor - main object for processing data
type Processor struct {
	workersCount         int
	workerJobsBufferSize int

	jobsChan          chan *job
	workersInProgress map[SyncKey]*worker
	releaseWorker     chan *worker
}

// SyncKey - all jobs with this key will be processing synchronously
type SyncKey = interface{}

// Callback for processing data
type Callback func(data interface{})

type job struct {
	syncKey  SyncKey
	data     interface{}
	callback Callback
}

type worker struct {
	syncKey       SyncKey
	jobs          chan *job
	releaseSignal chan *worker
}

// Create Processor
func Create(options Options) *Processor {
	p := &Processor{
		jobsChan:             make(chan *job),
		releaseWorker:        make(chan *worker),
		workerJobsBufferSize: options.WorkerJobsBufferSize,
		workersCount:         options.WorkersCount,
		workersInProgress:    make(map[SyncKey]*worker, options.WorkersCount),
	}
	for i := 0; i < options.WorkersCount; i++ {
		go func() {
			p.releaseWorker <- &worker{
				syncKey:       struct{}{},
				jobs:          make(chan *job, p.workerJobsBufferSize),
				releaseSignal: p.releaseWorker,
			}
		}()
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
		if worker, inProgress := p.workersInProgress[j.syncKey]; inProgress {
			worker.addJob(j)
		} else {
			releasedWorker := <-p.releaseWorker
			delete(p.workersInProgress, releasedWorker.syncKey)

			releasedWorker.syncKey = j.syncKey
			p.workersInProgress[releasedWorker.syncKey] = releasedWorker

			releasedWorker.addJob(j)
			go releasedWorker.work()
		}
	}
}

func (w *worker) addJob(j *job) {
	w.jobs <- j
}

func (w *worker) work() {
	for {
	loop:
		select {
		case j := <-w.jobs:
			j.callback(j.data)
		default:
			select {
			case w.releaseSignal <- w:
				return
			case j := <-w.jobs:
				j.callback(j.data)
				break loop
			}

		}
	}
}
