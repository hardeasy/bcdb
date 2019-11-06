package db

import (
	"bcdb/config"
	"testing"
)

func TestCalculationActiveFileNumber(t *testing.T){
	db := NewDb(config.Db.DataDir)
	number := db.Store.CalculationActiveFileNumber()
	t.Log(number)
}

func TestAdd(t *testing.T) {
	db := NewDb(config.Db.DataDir)
	_, _ , err := db.Store.Add("name", "dsadsa2321", 0)
	if err != nil {
		t.Fatal(err)
	}
}
