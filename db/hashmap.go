package db

import "sync"

//hash存储的块
type HashBlock struct {
	FileNumber int   //文件编号
	ValuePos   int64   //值的位置
	ValueLen   int   //值的长度
	ExpireAt  int   //到期时间戳 秒
}

type Hashmap struct {
	Data map[string]*HashBlock
	Lock sync.RWMutex
}

func NewHashmap() *Hashmap {
	return &Hashmap{
		Data: make(map[string]*HashBlock),
	}
}

func (self *Hashmap) Add(key string, value string, expireAt int) *HashBlock {
	self.Lock.Lock()
	defer self.Lock.Unlock()
	if v,ok := self.Data[key];ok{
		v.ValueLen = len(value)
		return v
	}
	hb := &HashBlock{
		ExpireAt: expireAt,
		ValueLen: len(value),
	}
	self.Data[key] = hb
	return hb
}

func (self *Hashmap) Get(key string) *HashBlock {
	self.Lock.RLock()
	defer self.Lock.RUnlock()

	return self.Data[key]
}
