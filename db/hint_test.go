package db

import (
	"bcdb/config"
	"testing"
)

func TestCreateHintFile(t *testing.T) {
	config.LoadConfig()
	db := NewDb(config.Db.DataDir)
	db.Init()
	CreateHintFile(db)
}

func TestLoadHintFile(t *testing.T) {
	config.LoadConfig()
	db := NewDb(config.Db.DataDir)
	db.Init()
	LoadHintFile(db)
}