package db

import (
	"errors"
)

type Db struct {
	Hashmap *Hashmap
	Store *Store
}

func NewDb(dataDir string) *Db{
	db := &Db{
		Hashmap: NewHashmap(),
	}
	db.Store = NewStore(db)
	return db
}

func (self *Db) Init() {
	self.LoadAllFileData()
}

func (self *Db) Add(key string, value string, expireAt int) error {
	hb := self.Hashmap.Add(key, value, expireAt)
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

	return nil
}

func (self *Db) Get(key string) (string, error){
	hb := self.Hashmap.Get(key)
	if hb == nil {
		return "", nil
	}

	value, err := self.Store.GetValue(hb.FileNumber, hb.ValuePos, hb.ValueLen)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (self *Db) LoadAllFileData() {
	fileBlockChan := make(chan FileBlock, 10)
	go func() {
		self.Store.LoadAllFileData(fileBlockChan)
	}()
	for fb :=range fileBlockChan {
		hb := self.Hashmap.Add(fb.Key, fb.Value, fb.ExpireAt)
		hb.FileNumber = fb.FileNumber
		hb.ValuePos = fb.ValuePos
		hb.ValueLen = fb.ValueLen
	}
}