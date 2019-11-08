package config

import "github.com/spf13/viper"

type cache struct {
	MaxSize string
}

var Cache *cache

func loadCacheConfig() {
	viper.SetDefault("cache.maxSize", "200M") //200 M
	Cache = &cache{
		MaxSize: viper.GetString("cache.maxSize"),
	}
}