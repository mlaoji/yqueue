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
	fmt.Printf("%#v", queue)
	for i := 0; i < 10000; i++ {
		rs, err := queue.AddTask("test", map[string]interface{}{"uid": i})

		fmt.Printf("%#v\n", rs)
		fmt.Printf("%#v", err)

	}
}
