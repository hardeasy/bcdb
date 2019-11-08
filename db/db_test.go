package db

import (
	"bcdb/config"
	"testing"
)

func TestDbAddGet(t *testing.T) {
	config.LoadConfig()
	db := NewDb(config.Db.DataDir)
	err := db.Add("name4", "12312313123", 0)
	if err != nil {
		t.Fatal(err)
	}

	value, exists, err := db.Get("name4")
	t.Log(value,exists)
}