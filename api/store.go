package api

import (
	"github.com/gin-gonic/gin"
	"bcdb/db"
	"time"
)

type store struct {

}
var Store *store = &store{}


type setRequest struct {
	Value string
	Expire int
}

func (self *store) Set(c *gin.Context) {
	key := c.Param("key")
	if len(key) == 0 {
		c.String(400, "params key not empty")
		return
	}
	request := &setRequest{}
	if err := c.ShouldBindJSON(request); err != nil {
		c.String(400, "body error")
		return
	}

	expireAt := 0
	if request.Expire > 0 {
		expireAt = int(time.Now().Unix()) + request.Expire
	}

	thisdb,ok := c.Keys["db"].(*db.Db)
	if !ok {
		c.String(500, "db error")
		return
	}

	err := thisdb.Add(key, request.Value, expireAt)
	if err != nil {
		c.String(500, err.Error())
	} else {
		c.String(200, "ok")
	}
}

func (self *store) Get(c *gin.Context) {
	key := c.Param("key")
	if len(key) == 0 {
		c.String(400, "params key not empty")
		return
	}
	thisdb,ok := c.Keys["db"].(*db.Db)
	if !ok {
		c.String(500, "db error")
		return
	}

	value, exists, err := thisdb.Get(key)
	if err != nil {
		c.String(500, err.Error())
		return
	}
	if !exists {
		c.String(404, "not found")
		return
	}

	c.String(200, value)
}

func (self *store) Delete(c *gin.Context) {
	key := c.Param("key")
	if len(key) == 0 {
		c.String(400, "params key not empty")
		return
	}
	thisdb,ok := c.Keys["db"].(*db.Db)
	if !ok {
		c.String(500, "db error")
		return
	}
	thisdb.Delete(key)
	c.String(200, "ok")
}

