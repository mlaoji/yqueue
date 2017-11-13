package yqueue

import (
	"bytes"
	"fmt"
	ylib "github.com/mlaoji/ygo/lib"
	"github.com/mlaoji/yqueue/lib"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"syscall"
	"time"
)

var (
	/*
		//alimns 配置
		DefaultQueueType   = "alimns"
		DefaultQueueConfig = map[string]string{
			"id":     "xxxxxxx",
			"secret": "xxxxxxx",
			"url":    "http://xxxxx.mns.cn-beijing.aliyuncs.com",
		}
	*/

	//redis配置
	DefaultQueueType    = "redis"
	DefaultQueueLogpath = "/data/logs/queue"
	DefaultQueueConfig  = map[string]string{
		"host":     "127.0.0.1:6379",
		"password": "test",
	}
)

type YQueue struct {
	Debug       bool
	Argvs       []string
	Worker      map[string]map[string]interface{}
	Handler     string
	queueType   string
	queueConfig []map[string]string
}

const (
	CMD_START   = "start"
	CMD_RESTART = "restart"
	CMD_STOP    = "stop"
	CMD_LIST    = "ls" //list process
	CMD_RESCUE  = "rs" //rescue task
)

func NewYQueue(options ...interface{}) *YQueue { //{{{//参数 queuetype, config... 多个配置时会随机散列
	l := len(options)
	qtype := DefaultQueueType
	qconf := []map[string]string{DefaultQueueConfig}
	if l > 0 {
		qtype = options[0].(string)
	}
	if l > 1 {
		configs := options[1:]
		qconf = []map[string]string{}
		for _, v := range configs {
			if conf, ok := v.(map[string]string); ok {
				qconf = append(qconf, conf)
			}
		}
	}

	lib.DefaultQueueLogpath = DefaultQueueLogpath

	return &YQueue{queueType: qtype, queueConfig: qconf}
} // }}}

func (this *YQueue) SetDebug(debug bool) { //{{{
	this.Debug = debug
} // }}}

func (this *YQueue) AddTask(job string, params map[string]interface{}) (traceid string, err error) { //{{{
	job_instance, err := lib.NewJob(job, this.queueType, this.queueConfig)
	if nil != err {
		return "", err
	}
	return job_instance.AddTask(params)
} // }}}

func (this *YQueue) AddWorker(job string, worker interface{}, max_children int) { //{{{
	if nil == this.Worker {
		this.Worker = make(map[string]map[string]interface{})
	}

	this.Worker[strings.ToLower(job)] = map[string]interface{}{"worker": worker, "max_children": max_children}
} // }}}

func (this *YQueue) Run() { //{{{
	dir := path.Dir(os.Args[0])
	os.Chdir(dir)
	this.Handler = dir + "/" + path.Base(os.Args[0])

	this.Argvs = os.Args[1:]

	cmd := ""
	op_job := ""
	len_argvs := len(this.Argvs)
	for k, v := range this.Argvs {
		cmd = strings.ToLower(this.Argvs[k])
		if cmd == CMD_START || cmd == CMD_RESTART || cmd == CMD_STOP || cmd == CMD_LIST || cmd == CMD_RESCUE {
			if len_argvs > k+1 {
				op_job = strings.ToLower(this.Argvs[k+1])
			}
			break
		} else {
			this.Handler += fmt.Sprintf(" %s", v)
		}
	}

	if "" != op_job && nil == this.Worker[op_job] {
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
	case CMD_RESCUE:
		this.rescueJob(op_job)
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

	for job, pid := range workers {
		if "" != op_job && job != op_job {
			continue
		}

		killed := this.stopProcess(ylib.ToInt(pid))
		fmt.Print("send signal to process, pid:" + pid)
		if killed {
			fmt.Println("success")
		} else {
			fmt.Println("failed")
		}

		fmt.Println("job:" + job + " pid:" + pid + " is stopping...")
	}

	done := false
	for !done {
		done = true

		for job, pid := range workers {
			if "" != op_job && job != op_job {
				continue
			}

			if this.isAliveProcess(ylib.ToInt(pid)) {
				done = false
			} else {
				delete(workers, job)
				fmt.Println("job:" + job + " is stopped!")
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
	rs, err := exec.Command("sh", "-c", "ps -ef|grep '_yqueue_' | grep ' restart \\| stop ' | grep -v grep |  grep -v "+ylib.ToString(syscall.Getpid())).Output()
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

func (this *YQueue) rescueJob(op_job string) { // {{{
} // }}}

func (this *YQueue) getMainWorkers(op_job string) map[string]string { // {{{
	cmd := ""
	if "" == op_job {
		cmd = "ps -ef|grep '_yqueue_'|grep ' start ' | grep -v grep|awk '{print $2\" \"$(NF-2)}'"
	} else {
		cmd = "ps -ef|grep '_yqueue_'|grep ' start '| grep '" + op_job + "' | grep -v grep|awk '{print $2\" \"$(NF-2)}'"
	}
	rs, err := exec.Command("sh", "-c", cmd).Output()
	pids := make(map[string]string)
	if nil == err && len(rs) > 0 {
		lines := strings.Split(string(rs), "\n")

		for _, line := range lines {
			if "" != line {
				pid_ppid := strings.Split(line, " ")
				pids[pid_ppid[1]] = pid_ppid[0]
			}
		}
	}

	return pids
} // }}}

func (this *YQueue) getWorkers() string { // {{{
	cmd := "ps -ef|grep '_yqueue_'|grep ' start '| grep -v grep"
	rs, err := exec.Command("sh", "-c", cmd).Output()
	if nil == err && len(rs) > 0 {
		return string(rs)
	}

	return ""
} // }}}

func (this *YQueue) startJob(op_job string, is_restart bool) { // {{{
	is_child_process := false
	l := len(this.Argvs)
	if l > 1 {
		r := bytes.NewReader([]byte(this.Argvs[l-2]))
		is_child_process, _ = regexp.MatchReader("_([0-9]+)_", r)
	}

	var live_workers map[string]string
	if !is_restart && !is_child_process {
		live_workers = this.getMainWorkers(op_job)
	}

	for job, worker := range this.Worker {
		if "" != op_job && job != op_job {
			continue
		}

		if !is_child_process {
			if this.isStoppingJob() {
				fmt.Println("job:" + job + " is stopping or restarting with other process!")
				continue
			}

			if !is_restart && len(live_workers[job]) > 0 {
				fmt.Println("job:" + job + " is already running!")
				continue
			}
		}

		if is_child_process || this.Debug {
			job_instance, err := lib.NewJob(job, this.queueType, this.queueConfig)
			if nil != err {
				fmt.Println("Error:")
				fmt.Println(err)
				time.Sleep(1e9)
				return
			}

			if this.Debug {
				job_instance.SetDebug(true)
			}

			job_instance.StartWorker(worker)
		} else {
			fmt.Println("job:" + job + " is starting...")
			cmd := "nohup " + this.Handler + " " + CMD_START + " " + job + " _" + ylib.ToString(syscall.Getpid()) + "_ _yqueue_ >> /dev/null 2>&1 &"
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
	fmt.Println("Usage:\n " + this.Handler + " [options] <command> [job name] \n")
	fmt.Println(" options:\n")
	fmt.Println(" command:\n\tstart | stop | restart | ls | rs \n")
	fmt.Println(" job name:\n\tdo all jobs without it\n")
}
