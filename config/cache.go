package config

import "github.com/spf13/viper"

type cache struct {
	MaxSize int64
}

var Cache *cache

func loadCacheConfig() {
	viper.SetDefault("cache.maxSize", 1024 * 200) //200 M
	Cache = &cache{
		MaxSize: viper.GetInt64("cache.maxSize"),
	}
}