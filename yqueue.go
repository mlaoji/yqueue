package yqueue

import (
	"bytes"
	"fmt"
	ylib "github.com/mlaoji/ygo/lib"
	"github.com/mlaoji/yqueue/lib"
	"github.com/mlaoji/yqueue/lib/queueclient"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"syscall"
	"time"
)

const (
	VERSION = "v0.1.1"
)

var (
	/*
		//alimns 配置
		DefaultQueueType   = "alimns"
		DefaultQueueServerConfig = []map[string]string{
		map[string]string{
			"alimns_id":     "xxxxxxx",
			"alimns_secret": "xxxxxxx",
			"alimns_url":    "http://xxxxx.mns.cn-beijing.aliyuncs.com",
		},
		}
	*/

	//redis配置
	DefaultQueueType         = "redis"
	DefaultQueueLogpath      = "logs"
	DefaultQueueServerConfig = []map[string]string{
		map[string]string{
			"host":     "127.0.0.1:6379",
			"password": "",
		},
	}

	//每个worker, 默认启动的消费者数量
	DefaultWorkerOptionChildren = 10

	//消息被取出后从活跃状态变成非活跃状态后的持续时间(单位秒), 超过此时间未消费会重新扔回队列变为活跃状态,
	DefaultWorkerOptionVisible = 1200

	//消息重试次数, 超过后被清理, -1表示无限重试
	DefaultWorkerOptionRetry = -1

	//消息过期时长, 超过后被清理, 只针对消费失败重试的消息, 0 表示不过期
	DefaultWorkerOptionExpire = 0
)

type YQueue struct {
	debug       bool
	argvs       []string
	handler     string
	workers     map[string]*lib.Worker
	queueType   string
	queueConfig []map[string]string
}

type FuncOption func(yq *YQueue)

//NewYQueue 设置参数 queueType
func WithOptionQueueType(queue_type string) FuncOption {
	return func(yq *YQueue) {
		yq.queueType = queue_type
	}
}

//NewYQueue 设置参数 queueConfig
func WithOptionQueueConfig(queue_conf []map[string]string) FuncOption {
	return func(yq *YQueue) {
		yq.queueConfig = queue_conf
	}
}

type FuncTaskOption func(options *queueclient.MessageOption)

//AddTask 设置参数 delay
func WithTaskOptionDelay(delay int) FuncTaskOption {
	return func(options *queueclient.MessageOption) {
		options.Delay = delay
	}
}

type FuncWorkerOption func(options *queueclient.MessageOption)

//AddWorker 设置参数 children
func WithWorkerOptionChildren(children int) FuncWorkerOption {
	return func(options *queueclient.MessageOption) {
		options.Children = children
	}
}

//AddWorker 设置参数 visible
func WithWorkerOptionVisible(visible int) FuncWorkerOption {
	return func(options *queueclient.MessageOption) {
		options.Visible = visible
	}
}

//AddWorker 设置参数 retry
func WithWorkerOptionRetry(retry int) FuncWorkerOption {
	return func(options *queueclient.MessageOption) {
		options.Retry = retry
	}
}

//AddWorker 设置参数 expire
func WithWorkerOptionExpire(expire int) FuncWorkerOption {
	return func(options *queueclient.MessageOption) {
		options.Expire = expire
	}
}

const (
	CMD_START   = "start"
	CMD_RESTART = "restart"
	CMD_STOP    = "stop"
	CMD_LIST    = "ls" //list process
)

//参数options: queuetype, config... 多个配置时会随机散列
func NewYQueue(options ...FuncOption) *YQueue { //{{{
	lib.DefaultQueueLogpath = DefaultQueueLogpath
	yq := &YQueue{queueType: DefaultQueueType, queueConfig: DefaultQueueServerConfig}

	for _, opt := range options {
		opt(yq)
	}

	return yq
} // }}}

func (this *YQueue) SetDebug(debug bool) { //{{{
	this.debug = debug
} // }}}

//向 queue_name 发送一条消息
func (this *YQueue) AddTask(queue_name string, params map[string]interface{}, taskOptions ...FuncTaskOption) (traceid string, err error) { //{{{
	job_instance, err := lib.NewJob(queue_name, this.queueType, this.queueConfig)
	if nil != err {
		return "", err
	}

	defaultMsgOptions := &queueclient.MessageOption{
		Delay: 0,
	}

	for _, opt := range taskOptions {
		opt(defaultMsgOptions)
	}

	return job_instance.AddTask(params, defaultMsgOptions)
} // }}}

func (this *YQueue) AddWorker(queue_name string, handler interface{}, workerOptions ...FuncWorkerOption) { //{{{
	if nil == this.workers {
		this.workers = make(map[string]*lib.Worker)
	}

	defaultMsgOptions := &queueclient.MessageOption{
		Children: DefaultWorkerOptionChildren,
		Visible:  DefaultWorkerOptionVisible,
		Retry:    DefaultWorkerOptionRetry,
		Expire:   DefaultWorkerOptionExpire,
	}

	for _, opt := range workerOptions {
		opt(defaultMsgOptions)
	}

	this.workers[strings.ToLower(queue_name)] = &lib.Worker{Handler: handler, Options: defaultMsgOptions}
} // }}}

func (this *YQueue) Run() { //{{{
	dir := path.Dir(os.Args[0])
	os.Chdir(dir)
	this.handler = dir + "/" + path.Base(os.Args[0])

	this.argvs = os.Args[1:]

	cmd := ""
	op_job := ""
	len_argvs := len(this.argvs)
	for k, v := range this.argvs {
		cmd = strings.ToLower(this.argvs[k])
		if cmd == CMD_START || cmd == CMD_RESTART || cmd == CMD_STOP || cmd == CMD_LIST {
			if len_argvs > k+1 {
				op_job = strings.ToLower(this.argvs[k+1])
			}
			break
		} else {
			this.handler += fmt.Sprintf(" %s", v)
		}
	}

	if "" != op_job && nil == this.workers[op_job] {
		fmt.Println(op_job + " is not defined")
		return
	}

	switch cmd {
	case CMD_START:
		this.startJob(op_job, false)
	case CMD_RESTART:
		this.stopJob(op_job)
		this.startJob(op_job, true)
	case CMD_STOP:
		this.stopJob(op_job)
	case CMD_LIST:
		this.listJob()
	default:
		this.showHelp()
	}
} // }}}

func (this *YQueue) stopJob(op_jobs ...string) { // {{{
	op_job := ""
	if len(op_jobs) > 0 {
		op_job = op_jobs[0]
	}

	if this.isStoppingJob() {
		fmt.Println("stop failed! job is stopping or restarting with other process!")
		return
	}

	workers := this.getMainWorkers(op_job)
	if 0 == len(workers) {
		fmt.Println("not found!")
		return
	}

	stop_workers := map[string]string{}
	for pid, job := range workers {
		if "" != op_job && job != op_job {
			continue
		}

		killed := this.stopProcess(ylib.ToInt(pid))
		fmt.Print("send signal to process, pid:" + pid)
		if killed {
			fmt.Println("success")
			fmt.Println("job:" + job + " pid:" + pid + " is stopping...")
			stop_workers[pid] = job
		} else {
			fmt.Println("failed")
		}
	}

	done := false
	for !done {
		done = true

		for pid, job := range stop_workers {
			if "" != op_job && job != op_job {
				continue
			}

			if this.isAliveProcess(ylib.ToInt(pid)) {
				done = false
			} else {
				delete(workers, pid)
				fmt.Println("job:" + job + " pid:" + pid + " is stopped!")
			}
		}

		if done {
			fmt.Println("")
		} else {
			fmt.Printf(".")
			time.Sleep(1e9)
		}
	}
} // }}}

func (this *YQueue) isAliveProcess(pid int) bool { // {{{
	if err := syscall.Kill(pid, 0); err != nil {
		return false
	}

	return true
} // }}}

func (this *YQueue) isStoppingJob() bool { // {{{
	rs, err := exec.Command("sh", "-c", "ps -ef|grep '_go_ _yqueue_' | grep ' restart \\| stop ' | grep -v grep |  grep -v "+ylib.ToString(syscall.Getpid())).Output()
	if nil == err && len(rs) > 0 {
		list := strings.Split(string(rs), "\n")
		return len(list) > 0
	}

	return false
} // }}}

func (this *YQueue) stopProcess(pid int) bool { // {{{
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return false
	}

	return true
} // }}}

func (this *YQueue) listJob() { // {{{
	rs := this.getWorkers()
	fmt.Println(rs)
} // }}}

func (this *YQueue) getMainWorkers(op_job string) map[string]string { // {{{
	cmd := ""
	if "" == op_job {
		cmd = "ps -ef|grep '_go_ _yqueue_'|grep ' start ' | grep -v grep|awk '{print $2\" \"$(NF-3)}'"
	} else {
		cmd = "ps -ef|grep '_go_ _yqueue_'|grep ' start '| grep '" + op_job + "' | grep -v grep|awk '{print $2\" \"$(NF-3)}'"
	}
	rs, err := exec.Command("sh", "-c", cmd).Output()
	pids := make(map[string]string)
	if nil == err && len(rs) > 0 {
		lines := strings.Split(string(rs), "\n")

		for _, line := range lines {
			if "" != line {
				pid_ppid := strings.Split(line, " ")
				pids[pid_ppid[0]] = pid_ppid[1]
			}
		}
	}

	return pids
} // }}}

func (this *YQueue) getWorkers() string { // {{{
	cmd := "ps -ef|grep '_go_ _yqueue_'|grep ' start '| grep -v grep"
	rs, err := exec.Command("sh", "-c", cmd).Output()
	if nil == err && len(rs) > 0 {
		return string(rs)
	}

	return ""
} // }}}

func (this *YQueue) startJob(op_job string, is_restart bool) { // {{{
	is_child_process := false
	l := len(this.argvs)
	if l > 1 {
		r := bytes.NewReader([]byte(this.argvs[l-3]))
		is_child_process, _ = regexp.MatchReader("_([0-9]+)_", r)
	}

	var live_workers map[string]string
	if !is_restart && !is_child_process {
		live_workers = this.getMainWorkers(op_job)
	}

FIRST:
	for job, wk := range this.workers {
		if "" != op_job && job != op_job {
			continue
		}

		if !is_child_process {
			if this.isStoppingJob() {
				fmt.Println("job:" + job + " is stopping or restarting with other process!")
				continue
			}

			if !is_restart {
				for _, j := range live_workers {
					if j == job {
						fmt.Println("job:" + job + " is already running!")
						continue FIRST
					}
				}
			}
		}

		if is_child_process || this.debug {
			job_instance, err := lib.NewJob(job, this.queueType, this.queueConfig)
			if nil != err {
				fmt.Println("Error:")
				fmt.Println(err)
				time.Sleep(1e9)
				return
			}

			if this.debug {
				job_instance.SetDebug(true)
			}

			job_instance.StartWorker(wk)
		} else {
			fmt.Println("job:" + job + " is starting...")
			cmd := "nohup " + this.handler + " " + CMD_START + " " + job + " _" + ylib.ToString(syscall.Getpid()) + "_ _go_ _yqueue_ >> /dev/null 2>&1 &"
			rs, err := exec.Command("sh", "-c", cmd).Output()
			if nil != err {
				fmt.Println("Error:")
				fmt.Println(string(rs))
			} else {
				fmt.Println("sucess!")
			}
		}
	}
} // }}}

func (this *YQueue) showHelp() {
	fmt.Println("YQueue is MessageQueue Consumer Manage Tools\n")
	fmt.Println("Version:", VERSION, "\n")
	fmt.Println("Usage:\n " + this.handler + " [options] <command> [job name] \n")
	fmt.Println(" options:\n")
	fmt.Println(" command:\n\tstart | stop | restart | ls \n")
	fmt.Println(" job name:\n\tdo all jobs without it\n")
}
