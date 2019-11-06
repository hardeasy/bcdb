package db

import (
	"errors"
	"os"
)

type Db struct {
	DataDir string
	Hashmap *Hashmap
	Store *Store
}

func NewDb(dataDir string) *Db{
	if dataDir[len(dataDir)-1] != os.PathSeparator {
		dataDir = dataDir + string(os.PathSeparator)
	}
	db := &Db{
		DataDir: dataDir,
		Hashmap: NewHashmap(),
	}
	db.Store = NewStore(db)
	return db
}

func (self *Db) Add(key string, value string, expireAt int) error {
	hb := self.Hashmap.Add(key, value, expireAt)
	if hb == nil {
		return errors.New("add hash error")
	}
	fileNumber,valuePos, err := self.Store.Add(key, value, expireAt)
	if err != nil {
		return err
	}
	hb.FileNumber = fileNumber
	hb.ValuePos = valuePos

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