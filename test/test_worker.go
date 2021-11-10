package main

import (
	ylib "github.com/mlaoji/ygo/lib"
	"github.com/mlaoji/yqueue"
)

func main() {
	/*
		//======== 配置加载方式 1 ==========
			conf := []map[string]string{
				map[string]string{
					"host":     "127.0.0.1:6379",
					"password": "",
					"timeout":  "30",
				},
				map[string]string{
					"host":     "127.0.0.1:6380",
					"password": "",
					"timeout":  "30",
				},
			}

			yqueue.DefaultWorkerOptionChildren = 2
			yqueue.DefaultQueueLogpath = "/tmp/log/queue"

			queue := yqueue.NewYQueue(yqueue.WithOptionQueueConfig(conf))
		//==================================
	*/

	//======== 配置加载方式 2 ==========
	ylib.Conf.Init("./test.conf")

	queue_conf := ylib.Conf.GetAll("queue_conf")
	yqueue.DefaultQueueType = queue_conf["queue_type"]
	yqueue.DefaultQueueLogpath = queue_conf["logpath"]

	yqueue.DefaultQueueServerConfig = []map[string]string{}
	queue_cluster_confs := ylib.Conf.GetSlice("queue_conf", "queue_server_clusters")
	if len(queue_cluster_confs) == 0 {
		queue_cluster_confs = []string{"queue_server_conf"}
	}

	for _, v := range queue_cluster_confs {
		yqueue.DefaultQueueServerConfig = append(yqueue.DefaultQueueServerConfig, ylib.Conf.GetAll(v))
	}
	queue := yqueue.NewYQueue()
	//==================================

	queue.SetDebug(true)
	queue.AddWorker("test", &testWorker{}, yqueue.WithWorkerOptionChildren(1))

	queue.Run()
}

type testWorker struct {
}

func (this *testWorker) Execute(params map[string]interface{}) {
	if params["panic"] == 1 {
		panic(0)
	}
}
