package main

import (
	"github.com/gin-gonic/gin"
	"bcdb/api"
	"bcdb/config"
	"bcdb/db"
	"sync"
)

func preInit() {
	config.LoadConfig()
}

func apiServer(db *db.Db) {
	r := gin.New()

	r.Use(gin.Recovery())

	r.Use(func(c *gin.Context) {
		c.Set("db", db)
		c.Next()
	})

	r.GET("/" , func(c *gin.Context) {
		c.String(200,"hello")
	})

	r.GET("/:key", api.Store.Get)
	r.POST("/:key", api.Store.Set)

	r.Run()
}

func main() {
	preInit()
	db := db.NewDb(config.Db.DataDir)
	db.Init()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		apiServer(db)
		wg.Done()
	}()
	wg.Wait()
}
