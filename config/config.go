package config

import (
	"fmt"
	"github.com/spf13/viper"
	"os"
)

func LoadConfig() {
	viper.SetConfigType("yaml")

	configFile := "./config.yaml"
	if len(os.Getenv("configFile")) != 0{
		configFile = os.Getenv("configFile")
	}
	viper.SetConfigFile(configFile)

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	loadDbConfig()
	loadCacheConfig()
}