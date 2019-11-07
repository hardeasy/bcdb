package cache

import (
	"bcdb/config"
	"strconv"
	"testing"
)

func TestSave(t *testing.T) {
	config.LoadConfig()
	t.Log(config.Cache.MaxSize)
	for i := 0; i < 200000; i++ {
		key := "name" + strconv.Itoa(i)
		value := "vvvvvvvvvv" + strconv.Itoa(i)
		Cache.Set(key, value)
		getValue,_ := Cache.Get(key)
		if value != getValue{
			t.Fail()
		}
	}
	t.Log(Cache.Used,Cache.Size)
}