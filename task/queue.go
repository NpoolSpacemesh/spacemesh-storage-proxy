package task

import (
	"encoding/json"
	"sync"
	"time"

	log "github.com/EntropyPool/entropy-logger"
	"github.com/NpoolChia/chia-storage-proxy/db"
	"github.com/boltdb/bolt"
)

var (
	globalQueue Qer
)

const (
	DefaultQSize = 1 << 8
)

const (
	TaskErr uint8 = iota
	TaskTodo
	TaskFinish
	TaskDone
)

type Meta struct {
	Status    uint8  `json:"status"`
	Host      string `json:"host"`
	PlotURL   string `json:"plot_url"`
	FailURL   string `json:"fail_url"`
	FinishURL string `json:"finish_url"`
}

type queue struct {
	// 记录已经在队列中的
	added map[string]struct{}
	// TODO 默认这里的任务数量不会大于 1 << 8
	qsize    uint8
	q        chan Meta
	callback map[uint8]func(Meta)

	// lock
	lock sync.Mutex
}

type Qer interface {
	Add(Meta)
	AddCallBack(uint8, func(Meta))
	// TODO 清理已经是 DONE 的 key
	IsAdded(key string) bool

	// fetch
	fetch()
	// run
	run()
}

// 对外提供的方法
func Add(m Meta) {
	globalQueue.Add(m)
}
func AddCallBack(s uint8, f func(Meta)) {
	globalQueue.AddCallBack(s, f)
}
func IsAdded(key string) bool {
	return globalQueue.IsAdded(key)
}

// 初始化任务队列
func NewQueue(qsize int) {
	if qsize <= 0 {
		qsize = DefaultQSize
	}
	globalQueue = &queue{
		q:        make(chan Meta, qsize),
		added:    make(map[string]struct{}),
		callback: make(map[uint8]func(Meta)),
	}
	// 拉取数据的任务
	go globalQueue.fetch()
	// 执行任务
	go globalQueue.run()
}

// Add 添加数据
func (q *queue) Add(meta Meta) {
	q.lock.Lock()
	if _, ok := q.added[meta.PlotURL]; !ok {
		q.added[meta.PlotURL] = struct{}{}
	}
	// 假设队列足够长
	q.q <- meta
	q.lock.Unlock()
}

// AddCallBack 添加处理函数
func (q *queue) AddCallBack(status uint8, callback func(meta Meta)) {
	q.lock.Lock()
	q.callback[status] = callback
	q.lock.Unlock()
}

// IsAdded 校验已添加
func (q *queue) IsAdded(key string) bool {
	q.lock.Lock()
	_, ok := q.added[key]
	q.lock.Unlock()
	return ok
}

// DelKey 删除
func (q *queue) DelKey(key string) error {
	q.lock.Lock()
	delete(q.added, key)
	q.lock.Unlock()
	return nil
}

func (q *queue) run() {
	for {
		select {
		case m := <-q.q:
			go func() {
				defer delete(q.added, m.PlotURL)
				// 这里需要小心 可以使用 ok 形式
				q.callback[m.Status](m)
			}()
		}
	}
}

func (q *queue) fetch() {
	// 每五分钟拉取一次数据
	for range time.NewTicker(time.Second * 10).C {
		bdb, err := db.BoltClient()
		if err != nil {
			log.Errorf(log.Fields{}, "get bolt database client error %v", err)
			continue
		}

		if err := bdb.View(func(tx *bolt.Tx) error {
			bk := tx.Bucket(db.DefaultBucket)
			return bk.ForEach(func(k, v []byte) error {
				meta := Meta{}
				if err := json.Unmarshal(v, &meta); err != nil {
					log.Errorf(log.Fields{}, "fetch bolt data to queue error %v", err)
					return nil
				}
				if !IsAdded(meta.PlotURL) && meta.Status != TaskDone {
					// TODO 同步数据优化
					globalQueue.Add(meta)
				}
				return nil
			})
		}); err != nil {
			log.Errorf(log.Fields{}, "fetch bolt data to queue error %v", err)
		}
	}
}
