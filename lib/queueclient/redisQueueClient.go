package queueclient

import (
	"fmt"
	ylib "github.com/mlaoji/ygo/lib"
	"math/rand"
	"os"
	"strings"
	"time"
)

func NewRedisQueueClient(queue_config []map[string]string) RedisQueueClient { // {{{
	return RedisQueueClient{
		config: queue_config,
	}
} // }}}

type RedisQueueClient struct {
	config   []map[string]string
	sha      map[string]string
	pendSha  map[string]string
	delaySha map[string]string
}

func (this RedisQueueClient) SendMessage(queue_name string, params map[string]interface{}, options *MessageOption) (traceid string, err error) { // {{{
	traceid = this.getTraceId()

	message := map[string]interface{}{
		"traceid": traceid,
		"addtime": ylib.Now(),
		"params":  ylib.JsonEncode(params),
	}

	json_data := ylib.JsonEncode(message)

	if options.Delay > 0 {
		err = this.sendDelayMessage(queue_name, json_data, options.Delay)
	} else {
		err = this.sendNormalMessage(queue_name, json_data)
	}

	return
} // }}}

func (this RedisQueueClient) sendNormalMessage(queue_name string, data string) error { // {{{
	redis, _, err := this.getRedis()
	if nil != err {
		return err
	}

	return redis.Lpush(this.getKey(queue_name), data)
} // }}}

func (this RedisQueueClient) sendDelayMessage(queue_name string, data string, delay int) error { // {{{
	redis, _, err := this.getRedis()
	if nil != err {
		return err
	}

	now := ylib.Now()

	delay_time := delay
	if delay <= DefaultMaxDelayTime {
		delay_time = now + delay
	}

	if delay_time < now || delay_time > (now+DefaultMaxDelayTime) {
		return fmt.Errorf("invalid delay time")
	}

	return redis.Zadd(this.getDelayKey(queue_name), delay_time, data)
} // }}}

func (this RedisQueueClient) ReceiveMessage(queue_name string, consume_id int, options *MessageOption) (data map[string]interface{}, traceid string, receipt_handle interface{}, err error) { // {{{
	redis, config_key, err := this.getRedis(consume_id)
	if nil != err {
		return nil, "", nil, err
	}

	sha, err := this.getConsumeSha(queue_name, redis)

	if nil != err {
		return nil, "", nil, err
	}

	dokey := this.getDoingKey(queue_name)
	for {
		resp, err := redis.Call("evalsha", sha, 2, dokey, this.getPendingKey(queue_name), ylib.Now()+options.Visible)

		if nil != err {
			return nil, "", nil, err
		}

		res, err := resp.Str()

		if nil != err {
			if "response is nil" == fmt.Sprint(err) {
				key := this.getKey(queue_name)
				resp, err := redis.Call("brpoplpush", key, dokey, DefaultBlockTime)
				if nil != err {
					return nil, "", nil, err
				}

				_, err = resp.Str()
				if nil != err {
					if "response is nil" == fmt.Sprint(err) {
						return nil, "", nil, nil
					}

					return nil, "", nil, err
				}

				continue
			}

			return nil, "", nil, err
		}

		ret := ylib.JsonDecode(res)
		val, ok := ret.(map[string]interface{})
		if !ok || nil == val["params"] {
			return nil, "", nil, fmt.Errorf("response data format invalid!")
		}

		params, ok := ylib.JsonDecode(ylib.AsString(val["params"])).(map[string]interface{})
		if !ok {
			return nil, "", nil, fmt.Errorf("response data format invalid!!")
		}

		return params, val["traceid"].(string), []interface{}{config_key, res}, nil
	}
} // }}}

func (this RedisQueueClient) DeleteMessage(queue_name string, receipt_handle interface{}) error { // {{{
	rh, ok := receipt_handle.([]interface{})
	if !ok || len(rh) != 2 {
		return fmt.Errorf("receipt_handle format invalid!")
	}

	config_key := ylib.AsInt(rh[0])
	msg := ylib.AsString(rh[1])

	redis, _, err := this.getRedis(config_key)
	if nil != err {
		return err
	}

	pend_key := this.getPendingKey(queue_name)
	_, err = redis.Zrem(pend_key, msg)

	return err
} // }}}

func (this RedisQueueClient) RescuePendingQueue(queue_name string) (total int, err error) { /*{{{*/
	key := this.getKey(queue_name)
	pend_key := this.getPendingKey(queue_name)

	for k, _ := range this.config {
		redis, _, err := this.getRedis(k)
		if nil != err {
			return total, err
		}

		sha, err := this.getPendingSha(queue_name, redis)
		if nil != err {
			return total, err
		}

		res, err := redis.Call("evalsha", sha, 2, key, pend_key, ylib.Now())
		if nil != err {
			return total, err
		}

		t, err := res.Int()
		if nil != err {
			return total, err
		}

		total = total + t
	}

	return total, nil
} /*}}}*/

func (this RedisQueueClient) RescueDelayQueue(queue_name string) (total int, err error) { /*{{{*/
	key := this.getKey(queue_name)
	delay_key := this.getDelayKey(queue_name)

	for k, _ := range this.config {
		redis, _, err := this.getRedis(k)
		if nil != err {
			return total, err
		}

		sha, err := this.getDelaySha(queue_name, redis)
		if nil != err {
			return total, err
		}

		res, err := redis.Call("evalsha", sha, 2, key, delay_key, ylib.Now())
		if nil != err {
			return total, err
		}

		t, err := res.Int()
		if nil != err {
			return total, err
		}

		total = total + t
	}

	return total, nil
} /*}}}*/

func (this RedisQueueClient) GetConsumerLimit(queue_name string) int { //{{{
	return len(this.config)
} //}}}

func (this RedisQueueClient) RetryQueue(queue_name string, receipt_handle interface{}, options *MessageOption) error { /*{{{*/
	rh, ok := receipt_handle.([]interface{})
	if !ok || len(rh) != 2 {
		return fmt.Errorf("receipt_handle format invalid!")
	}

	config_key := ylib.AsInt(rh[0])
	msg := ylib.AsString(rh[1])

	data, ok := ylib.JsonDecode(msg).(map[string]interface{})
	if !ok {
		return fmt.Errorf("receipt_handle format invalid!!")
	}

	retry_times := ylib.AsInt(data["retry"])

	if (options.Retry >= 0 && retry_times >= options.Retry) || (options.Expire > 0 && (ylib.AsInt(data["addtime"])+options.Expire) <= ylib.Now()) {
		err := this.DeleteMessage(queue_name, receipt_handle)
		if nil != err {
			return err
		}

		return fmt.Errorf("message deleted: options.Retry=%d,retry_times=%d, options.Expire=%d, OverRetryTimes=%t, Expired=%t", options.Retry, retry_times, options.Expire, (options.Retry >= 0 && retry_times >= options.Retry), (options.Expire > 0 && (ylib.AsInt(data["addtime"])+options.Expire) <= ylib.Now()))
	}

	retry_intervals := strings.Split(DefaultRetryIntervals, ",")
	retry_interval := 0
	if len(retry_intervals) > retry_times {
		retry_interval = ylib.AsInt(retry_intervals[retry_times])
	}

	if retry_interval <= 0 || retry_interval >= options.Visible {
		return fmt.Errorf("ignored: retry_interval=%d, options.Visible=%d, (retry_interval <= 0)=%t, (retry_interval >= options.Visible)=%t", retry_interval, options.Visible, (retry_interval <= 0), (retry_interval >= options.Visible))
	}

	redis, _, err := this.getRedis(config_key)
	if nil != err {
		return err
	}

	pend_key := this.getPendingKey(queue_name)
	return redis.Zadd(pend_key, ylib.Now()+retry_interval, msg)
} /*}}}*/

func (this RedisQueueClient) getRedis(config_keys ...int) (*ylib.RedisClient, int, error) { //{{{
	config_key := 0

	l := len(this.config)

	if len(config_keys) > 0 {
		config_key = config_keys[0]
		if config_key >= l {
			config_key = config_key % l
		}
	} else if l > 1 {
		config_key = this.getRand(l)
	}

	redis, err := ylib.NewRedisClient(this.config[config_key])
	return redis, config_key, err
} //}}}

func (this RedisQueueClient) getRand(len int) int { //{{{
	rand.Seed(int64(time.Now().Nanosecond()))
	return rand.Intn(len)
} //}}}

func (this RedisQueueClient) getTraceId() string { //{{{
	h, err := os.Hostname()
	if nil != err {
		h = ylib.GetLocalIp()
	}

	return fmt.Sprintf("%d%d%d", ylib.Crc32(h)&0x7FFFFFF|0x8000000, time.Now().Nanosecond()&0x7FFFFFF|0x8000000, this.getRand(1000000000))
} //}}}

func (this RedisQueueClient) getKey(queue_name string) string { //{{{
	return fmt.Sprint("queue:", queue_name)
} //}}}

func (this RedisQueueClient) getDelayKey(queue_name string) string { //{{{
	return fmt.Sprint("queue:", queue_name, ":delay")
} //}}}

func (this RedisQueueClient) getPendingKey(queue_name string) string { //{{{
	return fmt.Sprint("queue:", queue_name, ":pend")
} //}}}

func (this RedisQueueClient) getDoingKey(queue_name string) string { //{{{
	return fmt.Sprint("queue:", queue_name, ":do")
} //}}}

func (this RedisQueueClient) getConsumeSha(queue_name string, redis *ylib.RedisClient) (string, error) { // {{{
	if nil == this.sha {
		this.sha = map[string]string{}
	}

	sha, ok := this.sha[queue_name]
	if ok {
		resp, err := redis.Call("script", "exists", sha)
		if nil != err {
			ok = false
		} else {
			res, err := resp.Int()
			if nil != err || res != 1 {
				ok = false
			}
		}
	}

	if !ok {
		lua := `
		local key, pend_key, next_time = KEYS[1], KEYS[2], ARGV[1]
		local item = redis.call('rpop', key)

		redis.log(redis.LOG_DEBUG, 'yq:getNormalSha:'..key..':'..cjson.encode(item))

		if item then
			redis.call('zadd', pend_key, next_time, item)
		end

		return item`

		resp, err := redis.Call("script", "load", lua)
		if nil != err {
			return "", err
		}

		res, err := resp.Str()
		if nil != err {
			return "", err
		}

		this.sha[queue_name] = res
	}

	return this.sha[queue_name], nil
} // }}}

func (this RedisQueueClient) getPendingSha(queue_name string, redis *ylib.RedisClient) (string, error) { // {{{
	if nil == this.pendSha {
		this.pendSha = map[string]string{}
	}

	sha, ok := this.pendSha[queue_name]
	if ok {
		resp, err := redis.Call("script", "exists", sha)
		if nil != err {
			ok = false
		} else {
			res, err := resp.Int()
			if nil != err || res != 1 {
				ok = false
			}
		}
	}

	if !ok {
		lua := `
		local key, pend_key, time = KEYS[1], KEYS[2], ARGV[1]
		local t = 0

		while(true) do	
			local value = redis.call("zrangebyscore", pend_key, 0, time, "LIMIT", "0", "1000")

			redis.log(redis.LOG_DEBUG, 'yq:getRescueSha:'..key..':'..cjson.encode(value))

			if not value or not value[1] then
				break	
			end

			for i, v in ipairs(value) do
				local item = cjson.decode(v)

				if item then
					if not item["retry"] then
						item["retry"] = 0
					end
					item["retry"] = item["retry"] + 1
					local nv = cjson.encode(item)
					redis.call('lpush', key, nv)
					redis.call("zrem", pend_key, v)
					t = t + 1
				end
			end
		end

		return t`

		resp, err := redis.Call("script", "load", lua)
		if nil != err {
			return "", err
		}

		res, err := resp.Str()
		if nil != err {
			return "", err
		}

		this.pendSha[queue_name] = res
	}

	return this.pendSha[queue_name], nil
} // }}}

func (this RedisQueueClient) getDelaySha(queue_name string, redis *ylib.RedisClient) (string, error) { // {{{
	if nil == this.delaySha {
		this.delaySha = map[string]string{}
	}

	sha, ok := this.delaySha[queue_name]
	if ok {
		resp, err := redis.Call("script", "exists", sha)
		if nil != err {
			ok = false
		} else {
			res, err := resp.Int()
			if nil != err || res != 1 {
				ok = false
			}
		}
	}

	if !ok {
		lua := `
		local key, delay_key, time = KEYS[1], KEYS[2], ARGV[1]
		local t = 0

		while(true) do	
			local value = redis.call("zrangebyscore", delay_key, 0, time, "LIMIT", "0", "1000")

			redis.log(redis.LOG_DEBUG, 'yq:getDelaySha:'..key..':'..cjson.encode(value))

			if not value or not value[1] then
				break
			end

			for i, v in ipairs(value) do
				redis.call('lpush', key, v)
				redis.call("zrem", delay_key, v)
				t = t + 1
			end
		end

		return t`

		resp, err := redis.Call("script", "load", lua)
		if nil != err {
			return "", err
		}

		res, err := resp.Str()
		if nil != err {
			return "", err
		}

		this.delaySha[queue_name] = res
	}

	return this.delaySha[queue_name], nil
} // }}}
