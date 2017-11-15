package bcdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"
)

type Store struct {
	ActiveFileNumber int    //当前文件编号
	DataDir          string //数据文件
}

//文件块
type FileBlock struct {
	Timestamp int    //到期时间戳 秒
	KeyLen    int    //key长度
	ValueLen  int    //value长度
	Key       string //key数据
	Value     string //值
}

//返回valuepos位置
func (self *Store) Save(key string, value string, timestamp int) (int, int64, error) {
	fb := &FileBlock{
		Timestamp: timestamp,
		KeyLen:    len(key),
		ValueLen:  len(value),
		Key:       key,
		Value:     value,
	}
	filePath := self.GetActiveFilePath()
	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return 0, 0, err
	}
	defer fd.Close()

	buffer := bytes.NewBuffer([]byte{})
	binary.Write(buffer, binary.BigEndian, int64(fb.Timestamp))

	binary.Write(buffer, binary.BigEndian, int64(fb.KeyLen))
	binary.Write(buffer, binary.BigEndian, int64(fb.ValueLen))

	buffer.Write([]byte(fb.Key))
	buffer.Write([]byte(fb.Value))

	fmt.Println(buffer.Bytes())
	//当前offset
	curOffset, _ := fd.Seek(0, os.SEEK_END)
	//save
	_, err2 := fd.Write(buffer.Bytes())
	if err2 != nil {
		return 0, 0, err2
	}
	offset := curOffset + int64(unsafe.Sizeof(fb.Timestamp)) + int64(unsafe.Sizeof(fb.KeyLen)) + int64(unsafe.Sizeof(fb.ValueLen)) + int64(len(fb.Key))

	return self.ActiveFileNumber, offset, nil
}

func (self *Store) Get(fnumber int, offset int64, valueSize int) (string, error) {
	filePath := self.GetFilePath(fnumber)
	fd, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer fd.Close()

	_, err2 := fd.Seek(offset, os.SEEK_CUR)
	if err2 != nil {
		return "", err
	}

	r := bufio.NewReader(fd)
	buf := make([]byte, valueSize)
	r.Read(buf)

	return string(buf), nil
}

func (self *Store) GetActiveFilePath() string {
	return fmt.Sprintf("%s/db.%d", self.DataDir, self.ActiveFileNumber)
}

func (self *Store) GetFilePath(fnumber int) string {
	return fmt.Sprintf("%s/db.%d", self.DataDir, fnumber)
}

var store *Store = &Store{ActiveFileNumber: 0, DataDir: "./"}
