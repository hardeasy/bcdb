package db

import (
	"bcdb/config"
	"bcdb/tool"
	"os"
	"sort"
	"sync"
	"time"
	"unsafe"
)

type merge struct {
	db *Db
	runLock sync.Mutex
}

func newMerge(db *Db) *merge {
	merge := &merge{
		db: db,
	}
	return merge
}

func (self *merge) Run(nums []int) {
	self.runLock.Lock()
	defer self.runLock.Unlock()
	if self.db.IsRunMerge == true {
		return
	}

	self.db.IsRunMerge = true
	sort.Ints(nums)
	//sdb.lock
	os.Create(config.Db.DataDir+"sdb.lock")

	//sdb.n
	fdClose := false
	fd, err := os.OpenFile(config.Db.DataDir + "sdb", os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer func() {
		if fdClose == false {
			fd.Close()
		}
	}()
	for key,hb := range self.db.Hashmap[0].Data {
		if hb.ExpireAt == -1 || (hb.ExpireAt > 0 && int64(hb.ExpireAt) < time.Now().Unix()) {
			continue
		}
		//fmt.Println(key,hb)
		value, e := self.db.Store.GetValue(hb.FileNumber, hb.ValuePos, hb.ValueLen)
		if e != nil{
			continue
		}
		fb := self.db.Store.GetStoreFileBlock(key, value, hb.ExpireAt)
		bytes := self.db.Store.GetFileStoreBytes(fb)
		currOffset,_ := fd.Seek(0, os.SEEK_END)
		fd.Write(bytes)
		offset := currOffset +
			int64(unsafe.Sizeof(int64(fb.Crc))) +
			int64(unsafe.Sizeof(int64(fb.ExpireAt))) +
			int64(unsafe.Sizeof(int64(fb.KeyLen))) +
			int64(unsafe.Sizeof(int64(fb.ValueLen))) +
			int64(len(fb.Key))

		fb.FileNumber = -1 //快照
		fb.ValuePos = offset

		hb.FileNumber = fb.FileNumber
		hb.ValuePos = fb.ValuePos
		hb.ValueLen = fb.ValueLen
		hb.ExpireAt = fb.ExpireAt
	}

	os.Remove(config.Db.DataDir + "sdb.lock")
	for _, num := range nums {
		os.Remove(self.db.Store.GetFilePath(num))
	}

	tool.CopyFile(self.db.Store.GetFilePath(0), config.Db.DataDir + "sdb")

	//map指向
	for _,hb := range self.db.Hashmap[0].Data {
		hb.FileNumber = 0
	}
	fdClose = true
	fd.Close()

	os.Remove(config.Db.DataDir + "sdb")

	//hash merge
	for key,hb := range self.db.Hashmap[0].Data {
		newHb := self.db.Hashmap[1].Get(key)
		if newHb != nil {
			continue
		}
		self.db.Hashmap[1].Lock.Lock()
		self.db.Hashmap[1].Data[key] = hb
		self.db.Hashmap[1].Lock.Unlock()
	}

	self.db.Hashmap[0] = self.db.Hashmap[1]
	self.db.Hashmap[1] = NewHashmap()

	self.db.IsRunMerge = false
}
