package db

import (
	"fmt"
	"testing"
)

func TestSetAndGet(t *testing.T) {
	hashMap := NewHashmap()
	ht := hashMap.Add("name", "zhangshan", 0)
	p1 := fmt.Sprintf("%p",ht)

	ht2 := hashMap.Add("name", "zhangshan2", 0)
	p2 := fmt.Sprintf("%p",ht2)

	if p1 != p2 {
		t.Fail()
	}

	ht3 := hashMap.Add("name2", "zhangshan2", 0)
	p3 := fmt.Sprintf("%p",ht3)

	if (p1 == p3 || p2 == p3){
		t.Fail()
	}
}