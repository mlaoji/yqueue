package queueclient

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	ylib "github.com/mlaoji/ygo/lib"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"
)

const mns_version = "2015-06-06"

type MessageResponse struct {
	MessageId      string `xml:"MessageId" json:"message_id"`
	MessageBodyMD5 string `xml:"MessageBodyMD5" json:"message_body_md5"`
	ReceiptHandle  string `xml:"ReceiptHandle" json:"receipt_handle"`
}

type ErrorResponse struct {
	Code      string `xml:"Code,omitempty" json:"code,omitempty"`
	Message   string `xml:"Message,omitempty" json:"message,omitempty"`
	RequestId string `xml:"RequestId,omitempty" json:"request_id,omitempty"`
	HostId    string `xml:"HostId,omitempty" json:"host_id,omitempty"`
}

type Message struct {
	MessageBody  base64Bytes `xml:"MessageBody"`
	DelaySeconds int64       `xml:"DelaySeconds"`
	Priority     int64       `xml:"Priority"`
}

type MessageReceive struct {
	MessageId        string      `xml:"MessageId"`
	ReceiptHandle    string      `xml:"ReceiptHandle" json:"receipt_handle"`
	MessageBody      base64Bytes `xml:"MessageBody" json:"message_body"`
	MessageBodyMD5   string      `xml:"MessageBodyMD5" json:"message_body_md5"`
	EnqueueTime      int64       `xml:"EnqueueTime" json:"enqueue_time"`
	NextVisibleTime  int64       `xml:"NextVisibleTime" json:"next_visible_time"`
	FirstDequeueTime int64       `xml:"FirstDequeueTime" json:"first_dequeue_time"`
	DequeueCount     int64       `xml:"DequeueCount" json:"dequeue_count"`
	Priority         int64       `xml:"Priority" json:"priority"`
}

type base64Bytes []byte

func (b base64Bytes) MarshalXML(e *xml.Encoder, start xml.StartElement) error { // {{{
	return e.EncodeElement(base64.StdEncoding.EncodeToString(b), start)
} // }}}

func (b *base64Bytes) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) { // {{{
	var content string
	if e := d.DecodeElement(&content, &start); e != nil {
		return e
	}

	if buf, e := base64.StdEncoding.DecodeString(content); e != nil {
		return e
	} else {
		*b = base64Bytes(buf)
	}

	return nil
} // }}}

func NewAlimnsClient(queue_config map[string]string) AlimnsClient {
	timeout := DefaultTimeout
	if len(queue_config["timeout"]) > 0 {
		timeout = ylib.AsInt(queue_config["timeout"])
	}

	return AlimnsClient{appid: queue_config["alimns_id"], secret: queue_config["alimns_secret"], url: queue_config["alimns_url"], timeout: timeout}
}

var DefaultTimeout int = 5

type AlimnsClient struct {
	appid   string
	secret  string
	url     string
	timeout int
}

func (this AlimnsClient) request(method string, path string, data []byte) (resp *http.Response, err error) { // {{{
	var req *http.Request

	if req, err = http.NewRequest(method, this.url+path, bytes.NewBuffer(data)); err != nil {
		return nil, err
	}

	headers := map[string]string{}
	headers["x-mns-version"] = mns_version
	headers["Content-Type"] = "application/xml"
	headers["Date"] = time.Now().UTC().Format(http.TimeFormat)

	if data != nil {
		headers["Content-MD5"] = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%x", md5.Sum(data))))
	}

	sign, err := signature(this.secret, method, headers, path)
	if err != nil {
		return nil, err
	}

	headers["Authorization"] = "MNS " + this.appid + ":" + sign

	for header, value := range headers {
		req.Header.Add(header, value)
	}

	client := &http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				conn, err := net.DialTimeout(netw, addr, time.Second*time.Duration(this.timeout))
				if err != nil {
					return nil, err
				}
				conn.SetDeadline(time.Now().Add(time.Second * time.Duration(this.timeout)))
				return conn, nil
			},
			ResponseHeaderTimeout: time.Second * time.Duration(this.timeout),
			DisableKeepAlives:     true,
		},
	}

	return client.Do(req)
} // }}}

func (this AlimnsClient) respHandler(method string, path string, data []byte, v interface{}) (err error) { // {{{
	resp, err := this.request(method, path, data)
	if err != nil {
		return err
	}

	//error
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		decoder := xml.NewDecoder(resp.Body)
		e := ErrorResponse{}
		decoder.Decode(&e)
		return fmt.Errorf(e.Message)
	}

	if v != nil {
		decoder := xml.NewDecoder(resp.Body)
		return decoder.Decode(v)
	}

	return
} // }}}

func signature(key string, method string, headers map[string]string, resource string) (signature string, err error) { // {{{
	contentMD5 := ""
	contentType := ""
	date := time.Now().UTC().Format(http.TimeFormat)

	if v, exist := headers["Content-MD5"]; exist {
		contentMD5 = v
	}

	if v, exist := headers["Content-Type"]; exist {
		contentType = v
	}

	if v, exist := headers["Date"]; exist {
		date = v
	}

	mnsHeaders := []string{}

	for k, v := range headers {
		if strings.HasPrefix(k, "x-mns-") {
			mnsHeaders = append(mnsHeaders, k+":"+strings.TrimSpace(v))
		}
	}

	sort.Sort(sort.StringSlice(mnsHeaders))

	stringToSign := string(method) + "\n" +
		contentMD5 + "\n" +
		contentType + "\n" +
		date + "\n" +
		strings.Join(mnsHeaders, "\n") + "\n" +
		resource

	sha1Hash := hmac.New(sha1.New, []byte(key))
	if _, e := sha1Hash.Write([]byte(stringToSign)); e != nil {
		panic(e)
		return
	}

	signature = base64.StdEncoding.EncodeToString(sha1Hash.Sum(nil))

	return
} // }}}

func (this AlimnsClient) SendMessage(queue_name string, params map[string]interface{}, options *MessageOption) (traceid string, err error) { // {{{
	delay_seconds := int64(options.Delay)
	priority := int64(8)

	msg := Message{MessageBody: []byte(ylib.JsonEncode(params)), DelaySeconds: delay_seconds, Priority: priority}
	msg_xml_bytes, _ := xml.Marshal(&msg)
	path := fmt.Sprintf("/queues/%s/messages", queue_name)
	m := MessageResponse{}
	err = this.respHandler("POST", path, msg_xml_bytes, &m)
	if nil != err {
		return "", err
	}

	return m.MessageId, nil
} // }}}

func (this AlimnsClient) ReceiveMessage(queue_name string, _ int, options *MessageOption) (data map[string]interface{}, traceid string, receipt_handle interface{}, err error) { // {{{
	path := ""
	if DefaultBlockTime > 0 {
		path = fmt.Sprintf("/queues/%s/messages?waitseconds=%v", queue_name, DefaultBlockTime)
	} else {
		path = fmt.Sprintf("/queues/%s/messages", queue_name)
	}
	msg := MessageReceive{}
	err = this.respHandler("GET", path, nil, &msg)
	if nil != err {
		return nil, "", nil, err
	}

	body_str := string(msg.MessageBody)
	if "" != body_str {
		data, ok := ylib.JsonDecode(body_str).(map[string]interface{})
		if !ok {
			err = fmt.Errorf("response data is not map[string]interface{}")
			return nil, "", nil, err
		}
		return data, msg.MessageId, msg.ReceiptHandle, nil
	}

	return nil, "", nil, nil
} // }}}

func (this AlimnsClient) DeleteMessage(queue_name string, receipt_handle interface{}) error { // {{{
	path := fmt.Sprintf("/queues/%s/messages?ReceiptHandle=%s", queue_name, receipt_handle)
	return this.respHandler("DELETE", path, nil, nil)
} // }}}

func (this AlimnsClient) RescuePendingQueue(string) (int, error) { // {{{
	return 0, nil
} // }}}

func (this AlimnsClient) RescueDelayQueue(string) (int, error) { // {{{
	return 0, nil
} // }}}

func (this AlimnsClient) GetConsumerLimit(string) int { // {{{
	return 0
} // }}}

func (this AlimnsClient) RetryQueue(string, interface{}, *MessageOption) error { // {{{
	return nil
} // }}}
