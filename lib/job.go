package lib

import (
	"fmt"
	ylib "github.com/mlaoji/ygo/lib"
	"github.com/mlaoji/yqueue/lib/queueclient"
	"os"
	"os/signal"
	"reflect"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

type Job struct {
	JobName    string
	consumers  map[int]int
	queue      queueclient.QueueClient
	debug      bool
	sigChan    chan os.Signal
	stop       bool
	loggerRun  *ylib.FileLogger
	loggerJob  *ylib.FileLogger
	loggerTask *ylib.FileLogger
}

type Worker struct {
	Handler interface{}
	Options *queueclient.MessageOption
}

var (
	hookableSignals      []os.Signal
	DefaultQueueLogpath  = ""
	DefaultQueueLoglevel = 7
)

var yqueue_job_instance = map[string]*Job{}
var mutex sync.RWMutex

func NewJob(job_name, queue_type string, queue_config []map[string]string) (*Job, error) { // {{{
	key := job_name + queue_type
	mutex.RLock()
	if nil == yqueue_job_instance[key] {
		mutex.RUnlock()
		mutex.Lock()
		defer mutex.Unlock()

		if nil == yqueue_job_instance[key] {
			var err error
			yqueue_job_instance[key], err = newJob(job_name, queue_type, queue_config)
			if nil != err {
				return nil, err
			}
		}
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
		JobName:    job_name,
		sigChan:    make(chan os.Signal),
		loggerRun:  logger_run,
		loggerJob:  ylib.NewLogger(DefaultQueueLogpath+"/job", "job-"+job_name, DefaultQueueLoglevel),
		loggerTask: ylib.NewLogger(DefaultQueueLogpath+"/task", "task-"+job_name, DefaultQueueLoglevel),
		queue:      queue,
	}

	fmt.Println("job init:", "name =>", job_name, "type =>", queue_type)

	return job, nil
} // }}}

func (this *Job) AddTask(params map[string]interface{}, options *queueclient.MessageOption) (string, error) { // {{{
	start_time := time.Now()

	traceid, err := this.queue.SendMessage(this.JobName, params, options)
	consume := time.Now().Sub(start_time).Nanoseconds() / 1000 / 1000
	if nil != err {
		this.logTaskError(map[string]interface{}{"consume": consume, "params": params, "options": options, "error": err})
		return "", err
	}

	this.logTask(map[string]interface{}{"consume": consume, "params": params, "options": options, "traceid": traceid})

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

func (this *Job) StartWorker(worker *Worker) { // {{{
	var wg sync.WaitGroup

	defer this.getLastError()

	go this.handleSignals()
	go this.runPendingWorker(&wg)
	go this.runDelayWorker(&wg)

	/*
		if this.debug {
			fmt.Println("start in debug model")

			this.runWorker(worker, 0, nil, &wg)

			wg.Wait()

			fmt.Println("Exit")
			return
		}
	*/

	pid := syscall.Getpid()
	this.logRun("start main process, pid:", pid)

	total_consumers := worker.Options.Children
	if total_consumers < 1 {
		total_consumers = 1
	}

	consumer_limit := this.queue.GetConsumerLimit(this.JobName)
	if total_consumers < consumer_limit {
		total_consumers = consumer_limit
	}

	//计算缓冲区大小，阻塞最后一个worker，所以需要-1
	ch := make(chan int, total_consumers-1)

	go func() {
		for i := 0; i < total_consumers; i++ {
			ch <- i
		}
	}()

	for {
		if this.stop {
			this.logRun("worker stoped!")
			close(ch)
			break
		}

		select {
		case consumer_id, ok := <-ch:
			if ok {
				this.logRun("start child:", consumer_id)
				go this.runWorker(worker, consumer_id, ch, &wg)
			}
		default:
			time.Sleep(3e9)

		}
	}

	wg.Wait()

	this.println("Exit")
} // }}}

func (this *Job) runWorker(worker *Worker, consumer_id int, ch chan int, wg *sync.WaitGroup) { //{{{
	defer func() {
		this.getLastError()

		wg.Done()

		this.logRun("worker done child:", consumer_id)

		if nil != ch && !this.stop {
			ch <- consumer_id
		}
	}()

	wg.Add(1)

	for {
		if this.stop {
			this.logRun("stop child:", consumer_id)
			return
		}

		this.execTask(worker, consumer_id)
	}
} //}}}

func (this *Job) runPendingWorker(wg *sync.WaitGroup) { //{{{
	defer func() {
		this.getLastError()

		wg.Done()
	}()

	wg.Add(1)

	this.println("start pending worker!")

	for {
		if this.stop {
			this.logRun("pending worker stoped!")
			return
		}

		t, err := this.queue.RescuePendingQueue(this.JobName)

		if t > 0 {
			this.logRun("RescuePendingQueue total:", t)
		}

		if nil != err {
			this.logRunError("RescuePendingQueue error:", err)
		}

		time.Sleep(3e9)
	}
} //}}}

func (this *Job) runDelayWorker(wg *sync.WaitGroup) { //{{{
	defer func() {
		this.getLastError()

		wg.Done()
	}()

	wg.Add(1)

	this.println("start delay worker!")

	for {
		if this.stop {
			this.logRun("delay worker stoped!")
			return
		}

		t, err := this.queue.RescueDelayQueue(this.JobName)
		if t > 0 {
			this.logRun("RescueDelayQueue total:", t)
		}

		if nil != err {
			this.logRunError("RescueDelayQueue error:", err)
		}

		time.Sleep(3e9)
	}
} //}}}

func (this *Job) execTask(worker *Worker, consumer_id int) { //{{{
	params, traceid, receipt_handle, err := this.queue.ReceiveMessage(this.JobName, consumer_id, worker.Options)
	if nil != err {
		this.logRunError("ReceiveMessage error:", err)
		return
	}

	this.println("ReceiveMessage:", params)

	if nil == params {
		if queueclient.DefaultBlockTime == 0 {
			time.Sleep(3e9) // redis使用brpoplpush堵塞
		}
		return
	}

	//是否成功消费
	success := []bool{false}

	defer this.getLastError(traceid, params, receipt_handle, worker.Options, success)

	start_time := time.Now()
	worker_type := worker.Handler

	var workerType reflect.Type
	reflectVal := reflect.ValueOf(worker_type)
	workerType = reflect.Indirect(reflectVal).Type()
	vc := reflect.New(workerType)

	in := make([]reflect.Value, 1)
	in[0] = reflect.ValueOf(params)
	method := vc.MethodByName("Execute")
	res := method.Call(in)

	consume := time.Now().Sub(start_time).Nanoseconds() / 1000 / 1000

	if len(res) > 0 && !res[0].IsNil() {
		switch val := res[0].Interface().(type) {
		case bool:
			if !val {
				err = fmt.Errorf("worker function return false") //worker脚本返回false,消费失败
				this.logJobWarn(map[string]interface{}{"traceid": traceid, "consume": consume, "params": params, "err": err})
				return
			}
		case error: //worker脚本返回error,消费失败
			this.logJobWarn(map[string]interface{}{"traceid": traceid, "consume": consume, "params": params, "err": val})
			return
		default:
		}
	}

	//消费成功
	success[0] = true

	consume_t := time.Now().Sub(start_time).Nanoseconds() / 1000 / 1000
	this.logJob(map[string]interface{}{"traceid": traceid, "consume": consume, "consume_t": consume_t, "params": params})

	//向queue发送ack完成消费
	this.queue.DeleteMessage(this.JobName, receipt_handle)

	this.println("DeleteMessage:", this.JobName, receipt_handle)
} //}}}

func (this *Job) logRun(data ...interface{}) { // {{{
	this.loggerRun.Access(data...)
	this.println("logRun:", data)
} // }}}

func (this *Job) logRunError(data ...interface{}) { // {{{
	this.loggerRun.Error(data...)
	this.println("logRunError:", data)
} // }}}

func (this *Job) logJob(data ...interface{}) { // {{{
	this.loggerJob.Access(data...)
	this.println("logJob:", data)
} // }}}

func (this *Job) logJobError(data ...interface{}) { // {{{
	this.loggerJob.Error(data...)
	this.println("logJobError:", data)
} // }}}

func (this *Job) logJobWarn(data ...interface{}) { // {{{
	this.loggerJob.Warn(data...)
	this.println("logJobWarn:", data)
} // }}}

func (this *Job) logTask(data ...interface{}) { // {{{
	this.loggerTask.Access(data...)
} // }}}

func (this *Job) logTaskError(data ...interface{}) { // {{{
	this.loggerTask.Error(data...)
} // }}}

func (this *Job) println(data ...interface{}) { // {{{
	if this.debug {
		fmt.Println("time:"+ylib.DateTime(), data)
	}
} // }}}

func (this *Job) getLastError(data ...interface{}) { // {{{
	var traceid interface{}
	var params interface{}
	var receipt_handle interface{}
	var options *queueclient.MessageOption

	if len(data) > 0 {
		traceid = data[0]
	}

	if len(data) > 1 {
		params = data[1]
	}

	if len(data) > 2 {
		receipt_handle = data[2]
	}

	if len(data) > 3 {
		options = data[3].(*queueclient.MessageOption)
	}

	if err := recover(); err != nil {
		var errmsg string
		switch errinfo := err.(type) {
		case error:
			errmsg = errinfo.Error()
		default:
			errmsg = fmt.Sprint(errinfo)
		}

		if traceid != nil {
			this.logJobError(map[string]interface{}{"traceid": traceid, "params": params, "options": options, "receipt_handle": receipt_handle, "err": errmsg})
		}

		debug_trace := debug.Stack()

		this.logRunError("catch error:", errmsg)
		this.logRunError("debug trace:", string(debug_trace))
	}

	if len(data) > 4 {
		success := data[4].([]bool)
		if !success[0] {
			err := this.queue.RetryQueue(this.JobName, receipt_handle, options)
			if nil != err {
				this.logRunError("RetryQueue error:", map[string]interface{}{"receipt_handle": receipt_handle, "options": fmt.Sprintf("%#v", options)}, err)
				return
			}

			this.logRun("RetryQueue:", map[string]interface{}{"receipt_handle": receipt_handle, "options": fmt.Sprintf("%#v", options)})
		}
	}
} // }}}
