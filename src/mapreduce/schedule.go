package mapreduce

import (
	"fmt"
	"sync"
)


func scheduleOneWork(jobName string, mapFiles []string, phase jobPhase, n_other int,
	registerChan chan string, index int, idleWorkerChan chan string, waitGroup *sync.WaitGroup) {
	var worker string
	select {
		case worker = <-registerChan:
		case worker = <-idleWorkerChan:
	}
	arg := DoTaskArgs{jobName, mapFiles[index],
		phase,index,n_other}
	// if worker failed, re-schedule this task
	if ! call(worker, "Worker.DoTask", arg, nil) {
		go scheduleOneWork(jobName, mapFiles, phase, n_other, registerChan, index, idleWorkerChan, waitGroup)
		return
	}

	// avoid block here when no task to consumer this worker,
	select {
		case idleWorkerChan <- worker:
		default:
			break
	}
	waitGroup.Done()
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var waitGroup = new(sync.WaitGroup)

	idleWorkerChan := make(chan string)
	for i := 0 ; i < ntasks; i++ {
		waitGroup.Add(1)
		go scheduleOneWork(jobName, mapFiles, phase, n_other,
			registerChan, i, idleWorkerChan, waitGroup)
	}

	waitGroup.Wait()
	close(idleWorkerChan)

	fmt.Printf("Schedule: %v done\n", phase)
}
