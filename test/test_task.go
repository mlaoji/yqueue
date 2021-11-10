package main

import (
	"fmt"
	ylib "github.com/mlaoji/ygo/lib"
	"github.com/mlaoji/yqueue"
	"math/rand"
	"time"
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

	for i := 0; i < 10000; i++ {
		rand.Seed(int64(time.Now().Nanosecond()))
		delay := rand.Intn(30)
		panic := rand.Intn(10)

		params := map[string]interface{}{"id": i, "delay": delay, "panic": panic, "time": ylib.DateTime()}
		res, err := queue.AddTask("test", params, yqueue.WithTaskOptionDelay(delay))

		fmt.Println("params:", params)
		fmt.Println("res:", res)
		fmt.Println("err:", err)

		time.Sleep(3e9)
	}
}
