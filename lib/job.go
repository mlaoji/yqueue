package lib

import (
	"fmt"
	ylib "github.com/mlaoji/ygo/lib"
	"os"
	"os/signal"
	"reflect"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

type Job struct {
	Job        string
	queue      IQueue
	debug      bool
	sigChan    chan os.Signal
	stop       bool
	loggerRun  *ylib.FileLogger
	loggerJob  *ylib.FileLogger
	loggerTask *ylib.FileLogger
}

var (
	hookableSignals      []os.Signal
	DefaultQueueLogpath  = ""
	DefaultQueueLoglevel = 0
)

var yqueue_job_instance = map[string]*Job{}
var mutex sync.RWMutex

func NewJob(job_name, queue_type string, queue_config []map[string]string) (*Job, error) { // {{{
	key := job_name + queue_type
	mutex.RLock()
	if nil == yqueue_job_instance[key] {
		mutex.RUnlock()
		mutex.Lock()
		if nil == yqueue_job_instance[key] {
			var err error
			yqueue_job_instance[key], err = newJob(job_name, queue_type, queue_config)
			if nil != err {
				return nil, err
			}
		}
		mutex.Unlock()
	} else {
		mutex.RUnlock()
	}

	return yqueue_job_instance[key], nil
} // }}}

func newJob(job_name, queue_type string, queue_config []map[string]string) (*Job, error) { // {{{
	hookableSignals = []os.Signal{
		syscall.SIGINT,
		syscall.SIGTERM,
	}
	logger_run := ylib.NewLogger(DefaultQueueLogpath+"/run", "run-"+job_name, DefaultQueueLoglevel)
	queue, err := NewQueue(queue_type, queue_config)
	if nil != err {
		logger_run.Error(err)
		return nil, err
	}

	job := &Job{
		Job:        job_name,
		sigChan:    make(chan os.Signal),
		loggerRun:  logger_run,
		loggerJob:  ylib.NewLogger(DefaultQueueLogpath+"/job", "job-"+job_name, DefaultQueueLoglevel),
		loggerTask: ylib.NewLogger(DefaultQueueLogpath+"/task", "task-"+job_name, DefaultQueueLoglevel),
		queue:      queue,
	}

	fmt.Println("job init")
	return job, nil
} // }}}

func (this *Job) AddTask(params map[string]interface{}) (string, error) { // {{{
	start_time := time.Now()
	traceid, err := this.queue.AddQueue(this.Job, params)
	consume := time.Now().Sub(start_time).Nanoseconds() / 1000 / 1000
	if nil != err {
		this.logTaskError(map[string]interface{}{"consume": consume, "params": params, "error": err})
		return "", err
	}

	this.logTask(map[string]interface{}{"consume": consume, "params": params, "traceid": traceid})

	return traceid, nil
} // }}}

func (this *Job) SetDebug(debug bool) { // {{{
	this.debug = debug
} // }}}

func (this *Job) handleSignals() { // {{{
	var sig os.Signal

	signal.Notify(
		this.sigChan,
		hookableSignals...,
	)

	pid := syscall.Getpid()
	for {
		sig = <-this.sigChan
		switch sig {
		case syscall.SIGTERM, syscall.SIGINT:
			this.logRun("killed:", pid)

			this.stop = true
		default:
		}
	}
} // }}}

func (this *Job) StartWorker(worker map[string]interface{}) { // {{{
	var wg sync.WaitGroup

	if this.debug {
		fmt.Println("start in debug model")

		ch := make(chan int)
		this.runWorker(worker, ch, &wg)
		ch <- 1
		wg.Wait()

		fmt.Println("Exit")
		return
	}

	defer this.getLastError()

	go this.handleSignals()

	pid := syscall.Getpid()
	this.logRun("start main process, pid:", pid)

	//计算缓冲区大小，阻塞最后一个worker，所以需要-1
	total_chan := worker["max_children"].(int) - 1
	if total_chan < 0 {
		total_chan = 0
	}

	ch := make(chan int, total_chan)
	i := 0
	for {
		if i > total_chan {
			i = 0
		}
		i++

		if this.stop {
			this.logRun("worker stoped!")
			close(ch)
			break
		}

		this.logRun("start child:", i)
		go this.runWorker(worker, ch, &wg)
		ch <- i

		time.Sleep(3e9)
	}

	wg.Wait()

	fmt.Println("Exit")
} // }}}

func (this *Job) runWorker(worker map[string]interface{}, ch chan int, wg *sync.WaitGroup) { //{{{
	defer func() {
		this.getLastError()

		wg.Done()
	}()

	wg.Add(1)

	for {
		if this.stop {
			this.logRun("stop child:", <-ch)
			return
		}

		this.execTask(worker)
	}

	this.logRun("worker done child:", <-ch)
} //}}}

func (this *Job) execTask(worker map[string]interface{}) { //{{{
	params, traceid, receipt_handle, err := this.queue.GetQueue(this.Job)
	if nil != err {
		this.logRunError("get queue error:", err)
		return
	}

	if nil == params {
		time.Sleep(3e9)
		fmt.Println("wait...")
		return
	}

	defer this.getLastError()

	start_time := time.Now()
	worker_type := worker["worker"]

	var workerType reflect.Type
	reflectVal := reflect.ValueOf(worker_type)
	workerType = reflect.Indirect(reflectVal).Type()
	vc := reflect.New(workerType)

	in := make([]reflect.Value, 1)
	in[0] = reflect.ValueOf(params)
	method := vc.MethodByName("Execute")
	res := method.Call(in)
	var data interface{}
	if len(res) > 0 {
		data = res[0].Interface()
		if len(res) > 1 && !res[1].IsNil() {
			err = res[1].Interface().(error)
		}
	}

	consume := time.Now().Sub(start_time).Nanoseconds() / 1000 / 1000

	this.queue.ConsumeQueue(this.Job, receipt_handle)

	consume_t := time.Now().Sub(start_time).Nanoseconds() / 1000 / 1000
	this.logJob(map[string]interface{}{"traceid": traceid, "consume": consume, "consume_t": consume_t, "params": params, "data": data, "err": err})
} //}}}

func (this *Job) logRun(data ...interface{}) { // {{{
	this.loggerRun.Access(data...)
	fmt.Printf("%v", data)
} // }}}

func (this *Job) logRunError(data ...interface{}) { // {{{
	this.loggerRun.Error(data...)
	fmt.Printf("%v", data)
} // }}}

func (this *Job) logJob(data ...interface{}) { // {{{
	this.loggerJob.Access(data...)

	if this.debug {
		fmt.Printf("%v", data)
	}
} // }}}

func (this *Job) logJobError(data ...interface{}) { // {{{
	this.loggerJob.Error(data...)
} // }}}

func (this *Job) logTask(data ...interface{}) { // {{{
	this.loggerTask.Access(data...)
} // }}}

func (this *Job) logTaskError(data ...interface{}) { // {{{
	this.loggerTask.Error(data...)
} // }}}

func (this *Job) getLastError() { // {{{
	if err := recover(); err != nil {
		var errmsg string
		switch errinfo := err.(type) {
		case error:
			errmsg = errinfo.Error()
		default:
			errmsg = fmt.Sprint(errinfo)
		}

		debug_trace := debug.Stack()

		this.logRunError("catch error:", errmsg)
		this.logRunError("debug trace:", string(debug_trace))

		os.Stderr.Write(debug_trace)
	}
} // }}}
