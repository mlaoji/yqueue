package lib

import (
	"errors"
	"fmt"
	ylib "github.com/mlaoji/ygo/lib"
	"github.com/mlaoji/yqueue/lib/alimns"
	"math/rand"
	"os"
	"time"
)

const (
	QUEUE_TYPE_ALIMNS = "alimns"
	QUEUE_TYPE_REDIS  = "redis"
)

func NewQueue(queue_type string, queue_config []map[string]string) (IQueue, error) {
	if QUEUE_TYPE_ALIMNS != queue_type && QUEUE_TYPE_REDIS != queue_type {
		return nil, errors.New("queue type: " + queue_type + " is not exist")
	}

	if QUEUE_TYPE_ALIMNS == queue_type {
		config := queue_config[0]
		if 0 == len(config["id"]) || 0 == len(config["secret"]) || 0 == len(config["url"]) {
			return nil, errors.New("invalid config")
		}

		return AliMnsQueue{
			queueClient: alimns.NewAliMnsClient(config["id"], config["secret"], config["url"]),
		}, nil
	} else if QUEUE_TYPE_REDIS == queue_type {
		for _, v := range queue_config {
			if 0 == len(v["host"]) || 0 == len(v["password"]) {
				return nil, errors.New("invalid config")
			}
		}

		return RedisQueue{
			config: queue_config,
		}, nil
	}

	return nil, nil
}

type IQueue interface {
	AddQueue(string, map[string]interface{}) (string, error)
	GetQueue(string) (data map[string]interface{}, traceid, receipt_handle string, err error)
	ConsumeQueue(string, string) error
}

type AliMnsQueue struct {
	queueClient *alimns.AliMnsClient
}

func (this AliMnsQueue) AddQueue(job string, params map[string]interface{}) (string, error) { // {{{
	res, err := this.queueClient.SendMessage(job, ylib.JsonEncode(params))
	if nil != err {
		return "", err
	}

	return res.MessageId, nil
} // }}}

func (this AliMnsQueue) GetQueue(job string) (data map[string]interface{}, traceid, receipt_handle string, err error) { // {{{
	res, errs := this.queueClient.ReceiveMessage(job, 3)
	if nil != errs {
		err = errs
		return
	}

	body_str := string(res.MessageBody)
	if "" != body_str {
		body, ok := ylib.JsonDecode(body_str).(map[string]interface{})
		if !ok {
			err = errors.New("response data is not map[string]interface{}")
			return
		}
		data, traceid, receipt_handle = body, res.MessageId, res.ReceiptHandle
	}

	return
} // }}}

func (this AliMnsQueue) ConsumeQueue(job, receipt_handle string) error { // {{{
	return this.queueClient.DeleteMessage(job, receipt_handle)
} // }}}

type RedisQueue struct {
	config []map[string]string
}

func (this RedisQueue) AddQueue(job string, params map[string]interface{}) (string, error) { // {{{
	redis, err := this.getRedis()
	if nil != err {
		return "", err
	}

	traceid := this.getTraceId()

	message := map[string]interface{}{
		"traceid": traceid,
		"addtime": ylib.Now(),
		"retry":   0,
		"params":  params,
	}

	err = redis.Lpush(this.getKey(job), ylib.JsonEncode(message))
	if nil != err {
		return "", err
	}

	return traceid, nil
} // }}}

func (this RedisQueue) GetQueue(job string) (data map[string]interface{}, traceid, receipt_handle string, err error) { // {{{
	redis, err := this.getRedis()
	if nil != err {
		return nil, "", "", err
	}
	res, err := redis.Rpop(this.getKey(job))
	if nil != err {
		if "response is nil" == fmt.Sprint(err) {
			return nil, "", "", nil
		}
		return nil, "", "", err
	}

	ret := ylib.JsonDecode(res)
	val, ok := ret.(map[string]interface{})
	if !ok || nil == val["traceid"] || nil == val["addtime"] || nil == val["retry"] || nil == val["params"] {
		return nil, "", "", errors.New("response data format invalid")
	}

	params, ok := val["params"].(map[string]interface{})
	if !ok {
		return nil, "", "", errors.New("response data format invalid")
	}

	return params, val["traceid"].(string), "", nil
} // }}}

func (this RedisQueue) ConsumeQueue(job, receipt_handle string) error { // {{{
	return nil
} // }}}

func (this RedisQueue) getRedis() (*ylib.RedisClient, error) { //{{{
	rand.Seed(int64(time.Now().Nanosecond()))
	key := rand.Intn(len(this.config))

	return ylib.NewRedisClient(this.config[key])
} //}}}

func (this RedisQueue) getTraceId() string { //{{{
	h, err := os.Hostname()
	if nil != err {
		h = ylib.GetLocalIp()
	}
	return fmt.Sprintf("%d%d", ylib.Crc32(h)&0x7FFFFFF|0x8000000, time.Now().Nanosecond()&0x7FFFFFF|0x8000000)
} //}}}

func (this RedisQueue) getKey(job string) string { //{{{
	return fmt.Sprint("q:", job)
} //}}}
