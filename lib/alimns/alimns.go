package alimns

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
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

func NewAliMnsClient(appid, secret, url string) *AliMnsClient {
	timeout := DefaultTimeout
	return &AliMnsClient{appid: appid, secret: secret, url: url, timeout: timeout}
}

var DefaultTimeout int = 5

type AliMnsClient struct {
	appid   string
	secret  string
	url     string
	timeout int
}

func (this *AliMnsClient) request(method string, path string, data []byte) (resp *http.Response, err error) { // {{{
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

func (this *AliMnsClient) respHandler(method string, path string, data []byte, v interface{}) (err error) { // {{{
	resp, err := this.request(method, path, data)
	if err != nil {
		return err
	}

	//error
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		decoder := xml.NewDecoder(resp.Body)
		e := ErrorResponse{}
		decoder.Decode(&e)
		return errors.New(e.Message)
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

func (this *AliMnsClient) SendMessage(queueName, content string, options ...int64) (MessageResponse, error) { // {{{
	var delaySeconds int64 = 0
	var priority int64 = 8
	if l := len(options); l > 0 {
		delaySeconds = options[0]
		if l > 1 {
			priority = options[1]
		}
	}

	msg := Message{MessageBody: []byte(content), DelaySeconds: delaySeconds, Priority: priority}
	msgXmlBytes, _ := xml.Marshal(&msg)
	path := fmt.Sprintf("/queues/%s/messages", queueName)
	m := MessageResponse{}
	return m, this.respHandler("POST", path, msgXmlBytes, &m)
} // }}}

func (this *AliMnsClient) ReceiveMessage(queueName string, waitSec int64) (MessageReceive, error) { // {{{
	path := ""
	if waitSec < 0 {
		path = fmt.Sprintf("/queues/%s/messages", queueName)
	} else {
		path = fmt.Sprintf("/queues/%s/messages?waitseconds=%v", queueName, waitSec)
	}
	msg := MessageReceive{}
	return msg, this.respHandler("GET", path, nil, &msg)
} // }}}

func (this *AliMnsClient) DeleteMessage(queueName, receiptHandle string) error { // {{{
	path := fmt.Sprintf("/queues/%s/messages?ReceiptHandle=%s", queueName, receiptHandle)
	return this.respHandler("DELETE", path, nil, nil)
} // }}}
