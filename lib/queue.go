package lib

import (
	"fmt"
	"github.com/mlaoji/yqueue/lib/queueclient"
)

const (
	QUEUE_TYPE_ALIMNS = "alimns"
	QUEUE_TYPE_REDIS  = "redis"
)

func NewQueue(queue_type string, queue_config []map[string]string) (queueclient.QueueClient, error) { // {{{
	if 0 == len(queue_config) {
		return nil, fmt.Errorf("invalid config")
	}

	switch queue_type {
	case QUEUE_TYPE_REDIS:
		for _, v := range queue_config {
			if 0 == len(v["host"]) || 0 == len(v["password"]) {
				return nil, fmt.Errorf("invalid config")
			}
		}
		return queueclient.NewRedisQueueClient(queue_config), nil
	case QUEUE_TYPE_ALIMNS:
		conf := queue_config[0]
		if 0 == len(conf["alimns_id"]) || 0 == len(conf["alimns_secret"]) || 0 == len(conf["alimns_url"]) {
			return nil, fmt.Errorf("invalid config")
		}

		return queueclient.NewAlimnsClient(conf), nil
	default:
		return nil, fmt.Errorf("queue type: %s is not exist", queue_type)
	}

	return nil, nil
} // }}}
