package config

import (
	"fmt"
	"github.com/spf13/viper"
	"os"
)

type db struct {
	DataDir string
	MaxFileSize int64
}

var Db *db

func loadDbConfig() {
	viper.SetDefault("db.dataDir", "")
	Db = &db{
		DataDir: viper.GetString("db.datadir"),
	}
	if len(Db.DataDir) == 0 {
		panic(fmt.Errorf("config db.datadir not empty"))
	}
	if Db.DataDir[len(Db.DataDir)-1] != os.PathSeparator {
		Db.DataDir = Db.DataDir + string(os.PathSeparator)
	}

	viper.SetDefault("db.maxFileSize",1024)
	Db.MaxFileSize = viper.GetInt64("db.maxFileSize")
}