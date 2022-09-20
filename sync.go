package batch

import (
	"fmt"
	"sync"
	"time"
)

const (
	// ShardNum 分片个数
	ShardNum = 32
	// FlushNum 管道一次执行条数
	FlushNum = 6000
	// ItemLimit 内存中数量限制
	ItemLimit = 5000
	// ErrorNoticeNum 内存中触发报警条数
	ErrorNoticeNum = 20000
	// RepeatLimit 失败重试次数
	RepeatLimit = 3
)

// Message 消息结构体
type Message struct {
	key string
	val int64
}

// Shard 切片结构体
type Shard struct {
	items map[string]int64

	lock sync.Mutex
}

// Sync 异步操作结构体
type Sync struct {
	shards  []*Shard
	flushCh chan bool
	stopCh  chan bool
}

// NewSync 获取Sync结构体
func NewSync() (r *Sync) {
	shards := make([]*Shard, 0, ShardNum)
	for i := 0; i < ShardNum; i++ {
		shards = append(shards, &Shard{
			items: make(map[string]int64),
		})
	}

	return &Sync{
		shards:  shards,
		flushCh: make(chan bool),
		stopCh:  make(chan bool),
	}
}

// Start 程序启动入口
func (s *Sync) Start() {
	go s.startFlushLoop()
	go s.startCheckNumLoop()
}

func (s *Sync) startCheckNumLoop() {
	for {
		if s.count() >= ErrorNoticeNum {
			fmt.Printf("cache num beyond:%d", ErrorNoticeNum)
			s.flushShards()
		}
		time.Sleep(time.Second * 10)
	}
}

func (s *Sync) startFlushLoop() {
	interval := time.NewTicker(time.Second * 10)

	for {
		select {
		case <-s.flushCh:
			s.flushShards()
		case <-interval.C:
			s.flushShards()
		case <-s.stopCh:
			s.flushShards()
			break
		}
	}
}

func (s *Sync) flushShards() {
	items := s.getItems()

	if len(items) > 0 {
		// flush shards
	}
}

func (s *Sync) getItems() (r map[string]int64) {
	r = make(map[string]int64)
	ch := make(chan Message, s.count())

	go func() {
		wg := sync.WaitGroup{}
		wg.Add(ShardNum)

		for _, shard := range s.shards {
			go func(shard *Shard) {
				shard.lock.Lock()
				for k, v := range shard.items {
					ch <- Message{
						key: k,
						val: v,
					}
					delete(shard.items, k)
				}
				shard.lock.Unlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	for m := range ch {
		r[m.key] = m.val
	}
	return
}

func (s *Sync) getShard(key string) (r *Shard) {
	return s.shards[uint(s.fnv32(key))%uint(ShardNum)]
}

func (s *Sync) fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLen := len(key)
	for i := 0; i < keyLen; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (s *Sync) count() (r int) {
	for i := 0; i < ShardNum; i++ {
		shard := s.shards[i]
		shard.lock.Lock()
		r += len(shard.items)
		shard.lock.Unlock()
	}
	return
}

// Stop 程序优雅结束
func (s *Sync) Stop() {
	s.stopCh <- true
}

// Default 默认对象
var Default *Sync

// Start 默认程序启动入口
func Start() {
	Default = NewSync()
	Default.Start()
}

// Stop 默认程序优雅结束
func Stop() {
	Default.Stop()
}
