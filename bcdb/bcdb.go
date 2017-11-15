package bcdb

import (
	"fmt"
)

type Bcdb struct {
}

func NewBcdb() *Bcdb {
	return &Bcdb{}
}

func (self *Bcdb) Init() error {
	return nil
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
	v, _ := store.Get(hb.FileNumber, hb.ValuePos, hb.ValueLen)
	fmt.Println("v", v)

	return "okkk", false
}
