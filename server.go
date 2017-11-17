package main

import (
	"bcdb/bcdb"
	"encoding/json"
	"fmt"
	"net/http"
)

var b *bcdb.Bcdb

type Message struct {
	Status int8        `json:"status"`
	Msg    string      `json:"msg"`
	Data   interface{} `json:"data"`
}

func setHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	key := r.PostFormValue("key")
	value := r.PostFormValue("value")
	if len(key) == 0 {
		message := Message{Status: -1, Msg: "key Can not be empty"}
		b, _ := json.Marshal(message)
		w.Write(b)
		return
	}
	if len(value) == 0 {
		message := Message{Status: -1, Msg: "value Can not be empty"}
		b, _ := json.Marshal(message)
		w.Write(b)
		return
	}

	err := b.Set(key, value, 0)
	if err != nil {
		message := Message{Status: -1, Msg: "operation failed"}
		b, _ := json.Marshal(message)
		w.Write(b)
		return
	}
	message := Message{Status: 1, Msg: "ok"}
	b, _ := json.Marshal(message)
	w.Write(b)

}

func getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PostFormValue("key")
	if len(key) == 0 {
		message := Message{Status: -1, Msg: "key Can not be empty"}
		b, _ := json.Marshal(message)
		w.Write(b)
		return
	}
	value, ok := b.Get(key)
	if !ok {
		message := Message{Status: -1, Msg: "operation failed"}
		b, _ := json.Marshal(message)
		w.Write(b)
		return
	}
	message := Message{Status: 1, Msg: "ok", Data: value}
	b, _ := json.Marshal(message)
	w.Write(b)
}

func main() {
	b = bcdb.NewBcdb()

	http.HandleFunc("/set", setHandler)
	http.HandleFunc("/get", getHandler)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("error")
	}
	fmt.Println("run")
}
