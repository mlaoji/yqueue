package queueclient

var (
	//延迟队列最大支持延迟时间，单位秒, 默认30天
	DefaultMaxDelayTime = 2592000

	//重试时间间隔，多个逗号分隔，分别对应相应的次数, 未设置或超过visible设置的使用visible
	DefaultRetryIntervals = "1,3,5,10,15,30"

	//redis brpoplpush 的阻塞时间, 需要将redis配置中的timeout设置成大于此值
	DefaultBlockTime = 10
)

type QueueClient interface {
	SendMessage(string, map[string]interface{}, *MessageOption) (string, error)
	ReceiveMessage(string, int, *MessageOption) (data map[string]interface{}, traceid string, receipt_handle interface{}, err error)
	DeleteMessage(string, interface{}) error
	RescuePendingQueue(string) (int, error)
	RescueDelayQueue(string) (int, error)
	GetConsumerLimit(string) int
	RetryQueue(string, interface{}, *MessageOption) error
}

type MessageOption struct {
	Children int //consumer的数量
	Delay    int //延迟时间
	Visible  int //消息从非活跃变为活跃的时间
	Retry    int //重试次数
	Expire   int //有效期
}
