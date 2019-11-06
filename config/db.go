package config

import (
	"fmt"
	"github.com/spf13/viper"
)

type db struct {
	DataDir string
}

var Db *db

func loadDbConfig() {
	viper.SetDefault("db.datadir", "")
	Db = &db{
		DataDir: viper.GetString("db.datadir"),
	}
	if len(Db.DataDir) == 0 {
		panic(fmt.Errorf("config db.datadir not empty"))
	}
}