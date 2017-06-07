package main

import (
	"fmt"
	"github.com/mlaoji/yqueue"
)

func main() {
	conf := map[string]string{
		"host":     "127.0.0.1:6379",
		"password": "test",
	}

	conf1 := map[string]string{
		"host":     "127.0.0.1:6378",
		"password": "test",
	}
	queue := yqueue.NewYQueue("redis", conf, conf1)
	queue.SetDebug(true)
	queue.AddWorker("test", &testWorker{}, 10)

	queue.Run()
}

type testWorker struct {
}

func (this *testWorker) Execute(params map[string]interface{}) {
	fmt.Printf("%#v", params)
}
