package db

import (
	"bcdb/cache"
	"errors"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"
)

type Db struct {
	IsRunMerge bool
	Hashmap [2]*Hashmap
	Store *Store
	merge *merge
}

func NewDb(dataDir string) *Db{
	db := &Db{
		IsRunMerge: false,
		Hashmap: [2]*Hashmap{NewHashmap(), NewHashmap()},
	}
	db.Store = NewStore(db)
	db.merge = newMerge(db)
	return db
}

func (self *Db) Init() {
	self.preInit()
	self.signalHandler()

	//load data
	err := LoadHintFile(self)
	if err != nil {
		self.LoadAllFileData()
	}

}

func (self *Db) signalHandler() {
	//singal
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-signalChan
		self.Shutdown()
		os.Exit(0)
	}()
}

func (self *Db) Shutdown() {
	if self.IsRunMerge == false {
		CreateHintFile(self)
	}
}

func (self *Db) preInit() {
	//
	//rename db.*
	nums := self.Store.GetDataDirFileNumbers()
	sort.Ints(nums)
	if len(nums) == 0 {
		return
	}
	activeNum := nums[len(nums) - 1]
	renameFile := false
	for index,num := range nums {
		if num != index{
			os.Rename(self.Store.GetFilePath(num), self.Store.GetFilePath(index))
			renameFile = true
		}
		activeNum = index
	}
	self.Store.ActiveFileNumber = activeNum

	if renameFile {
		DeleteHintFile()
	}
}

func (self *Db) Add(key string, value string, expireAt int) error {
	var hb *HashBlock
	if self.IsRunMerge {
		hb = self.Hashmap[1].Add(key, value, expireAt)
	} else {
		hb = self.Hashmap[0].Add(key, value, expireAt)
	}
	if hb == nil {
		return errors.New("add hash error")
	}
	fb, err := self.Store.Add(key, value, expireAt)
	if err != nil {
		return err
	}
	hb.FileNumber = fb.FileNumber
	hb.ValuePos = fb.ValuePos
	hb.ValueLen = fb.ValueLen
	hb.ExpireAt = fb.ExpireAt

	go cache.Cache.Set(key, value)

	return nil
}

func (self *Db) Get(key string) (string, bool, error){
	var hb *HashBlock
	if self.IsRunMerge {
		hb = self.Hashmap[1].Get(key)
		if hb == nil {
			hb = self.Hashmap[0].Get(key)
		}
	} else {
		hb = self.Hashmap[0].Get(key)
	}

	if hb == nil {
		return "", false, nil
	}

	//expire
	if hb.ExpireAt == -1 || (hb.ExpireAt > 0 && hb.ExpireAt <= int(time.Now().Unix())) {
		return "", false, nil
	}

	//value
	if value,exists := cache.Cache.Get(key); exists {
		return value, true, nil
	}

	value, err := self.Store.GetValue(hb.FileNumber, hb.ValuePos, hb.ValueLen)
	if err != nil {
		return "", true, err
	}
	return value, true, nil
}

func (self *Db) Delete(key string) error {
	var hb *HashBlock
	if self.IsRunMerge {
		hb = self.Hashmap[1].Get(key)
		if hb == nil {
			hb = self.Hashmap[0].Get(key)
		}
	} else {
		hb = self.Hashmap[0].Get(key)
	}
	if hb == nil {
		return nil
	}

	err := self.Add(key, "", -1)
	if err != nil {
		return err
	}
	return nil
}

func (self *Db) LoadAllFileData() {
	fileBlockChan := make(chan FileBlock, 10)
	go func() {
		self.Store.LoadAllFileData(fileBlockChan)
	}()
	for fb :=range fileBlockChan {
		hb := self.Hashmap[0].Add(fb.Key, fb.Value, fb.ExpireAt)
		hb.FileNumber = fb.FileNumber
		hb.ValuePos = fb.ValuePos
		hb.ValueLen = fb.ValueLen
	}
}