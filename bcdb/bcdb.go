package bcdb

import (
	"fmt"
)

var store *Store
var hashmap *HashMap

type Bcdb struct {
}

func NewBcdb() *Bcdb {
	db := &Bcdb{}
	store = &Store{ActiveFileNumber: 0, DataDir: "./"}
	hashmap = &HashMap{Data: make(map[string]*HashBlock)}
	//扫描文件
	return db
}

func (self *Bcdb) Set(key string, value string, exp int) error {
	hb := hashmap.Add(key, len(value), exp)

	fnumber, offset, err := store.Save(key, value, exp)
	if err != nil {
		return err
	}
	fmt.Println("offset", offset, fnumber)

	hb.FileNumber = fnumber
	hb.ValuePos = offset

	return nil
}

func (self *Bcdb) Get(key string) (string, bool) {
	hb := hashmap.Get(key)
	if hb == nil {
		return "", false
	}
	v, err := store.Get(hb.FileNumber, hb.ValuePos, hb.ValueLen)
	fmt.Println("v", v)
	if err != nil {
		return "", false
	}

	return v, true
}
