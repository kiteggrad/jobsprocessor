package jobsprocessor

import (
	"reflect"
	"runtime"
	"sync"
	"testing"
)

type message struct {
	user int
	data int
}

const usersCount = 100
const messagesByUser = 1000

func Test(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var (
		result = map[int][]int{}
		m      = sync.Mutex{}

		queue    = initQueue(usersCount, messagesByUser)
		expected = generateExpectedData(usersCount, messagesByUser)
	)

	p := Create(Options{
		WorkerJobsBufferSize: 10,
		WorkersCount:         runtime.NumCPU(),
	})

	wg := sync.WaitGroup{}
	wg.Add(usersCount * messagesByUser)
	for mess := range queue {
		p.Process(mess.user, mess, func(data interface{}) {
			mess := data.(*message)
			m.Lock()
			result[mess.user] = append(result[mess.user], mess.data)
			wg.Done()
			m.Unlock()
		})
	}
	wg.Wait()

	if !reflect.DeepEqual(expected, result) {
		t.Fail()
	}
}

func generateExpectedData(usersCount, messagesByUser int) (data map[int][]int) {
	data = make(map[int][]int, usersCount)
	for i := 0; i < usersCount; i++ {
		data[i] = make([]int, messagesByUser)

		for k := 0; k < messagesByUser; k++ {
			data[i][k] = k
		}
	}

	return data
}

func initQueue(usersCount, messagesByUser int) (queue chan *message) {
	queue = make(chan *message, usersCount*messagesByUser)

	wg := sync.WaitGroup{}
	for i := 0; i < usersCount; i++ {
		wg.Add(messagesByUser)
		go func(i int) {
			for k := 0; k < messagesByUser; k++ {
				queue <- &message{i, k}
				wg.Done()
			}
		}(i)
	}
	wg.Wait()
	close(queue)

	return
}
