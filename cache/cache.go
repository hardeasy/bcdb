package cache

import (
	"bcdb/config"
	"bcdb/tool"
	"sort"
	"sync"
	"time"
)

type indexItem struct {
	Key string
	Length int
	LastUsedTime int64
}

type indexs []*indexItem
func (self indexs) Len() int {
	return len(self)
}
func (self indexs) Less(i, j int) bool {
	return self[i].LastUsedTime < self[i].LastUsedTime
}
func (self indexs) Swap(i, j int) {
	self[i],self[j] = self[j],self[i]
}



type cache struct {
	Used int64
	Size int  //个数
	Data map[string]string
	Indexs indexs
	Lock sync.Mutex
	recoverLock sync.Mutex
}

var Cache *cache = &cache{Data:make(map[string]string),Indexs:make([]*indexItem,0)}

func (self *cache) Set(key string, value string) {
	self.Lock.Lock()
	defer self.Lock.Unlock()
	if _,ok := self.Data[key]; ok {
		for i,_ := range self.Indexs {
			if self.Indexs[i].Key == key {
				self.Used -= int64(self.Indexs[i].Length)

				self.Indexs[i].LastUsedTime = time.Now().UnixNano()
				self.Indexs[i].Length = len(value)

				self.Used += int64(len(value))
			}
		}
		self.Data[key] = value
		return
	}

	indexItem := &indexItem{
		Key: key,
		Length: len(value),
		LastUsedTime: time.Now().UnixNano(),
	}

	self.Indexs = append(self.Indexs, indexItem)
	self.Data[key] = value
	self.Used += int64(indexItem.Length)
	self.Size++

	if (self.Used) > tool.StrToByteSize(config.Cache.MaxSize) {
		go func() {
			self.recover()
		}()
	}
}

func (self *cache) Get(key string) (string, bool) {
	self.Lock.Lock()
	defer self.Lock.Unlock()
	if value,ok := self.Data[key]; ok {
		index := self.searchIndexsKeyIndex(key)
		self.Indexs[index].LastUsedTime = time.Now().UnixNano()
		return value, true
	}
	return "", false
}

func (self *cache) Delete(key string) {
	self.Lock.Lock()
	defer self.Lock.Unlock()
	if _,ok := self.Data[key]; !ok {
		return
	}
	index := -1
	length := 0
	for i,_ := range self.Indexs {
		if self.Indexs[i].Key == key {
			index = i
			length = self.Indexs[i].Length
			break
		}
	}
	if index < 0 {
		return
	}
	self.Indexs = append(self.Indexs[0:index],self.Indexs[index+1:]...)
	delete(self.Data, key)
	self.Size--
	self.Used -= int64(length)
}

func (self *cache) searchIndexsKeyIndex(key string) int {
	for i,item := range self.Indexs {
		if item.Key == key {
			return i
		}
	}
	return -1
}

func (self *cache) recover() {
	oldUsed := self.Used
	self.recoverLock.Lock()
	defer self.recoverLock.Unlock()
	if oldUsed != self.Used {
		return
	}
	sort.Sort(self.Indexs)
	for self.Used > (tool.StrToByteSize(config.Cache.MaxSize)) && self.Size > 1 {
		self.Delete(self.Indexs[0].Key)
	}
}