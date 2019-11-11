package db

import (
	"bcdb/config"
	"bytes"
	"encoding/binary"
	"github.com/pkg/errors"
	"os"
	"unsafe"
)

func CreateHintFile(db * Db) error {
	lockFilePath := config.Db.DataDir + "hint.lock"
	lockFd,err := os.Create(lockFilePath)
	if err == nil{
		defer func() {
			lockFd.Close()
			os.Remove(lockFilePath)
		}()
	}

	fd,err := os.OpenFile(config.Db.DataDir + "hint", os.O_WRONLY | os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()
	for k,v := range db.Hashmap[0].Data {
		fbytes := getFileHashBlockByte(v, k)
		fd.Write(fbytes)
	}

	fd.Sync()
	return nil
}

func getFileHashBlockByte(hb *HashBlock, key string) []byte {
	buffer := bytes.NewBuffer([]byte{})
	binary.Write(buffer, binary.LittleEndian, int64(hb.FileNumber))
	binary.Write(buffer, binary.LittleEndian, int64(hb.ExpireAt))
	binary.Write(buffer, binary.LittleEndian, int64(hb.ValuePos))
	binary.Write(buffer, binary.LittleEndian, int64(hb.ValueLen))
	binary.Write(buffer, binary.LittleEndian, int64(len(key)))
	buffer.Write([]byte(key))
	return buffer.Bytes()
}

func LoadHintFile(db *Db) error {
	lockFilePath := config.Db.DataDir + "hint.lock"
	hintFilePath := config.Db.DataDir + "hint"
	lockFd, err := os.OpenFile(lockFilePath, os.O_RDONLY, 0644)
	if err == nil {
		lockFd.Close()
		os.Remove(lockFilePath)
		os.Remove(hintFilePath)
		return errors.New("hint.lock exists")
	}

	fd,err := os.OpenFile(hintFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	for {
		var fileNumber, expireAt, valuePos, valueLen, keyLen int64

		buf := make([]byte, unsafe.Sizeof(fileNumber))

		_, err := fd.Read(buf)
		if err != nil {
			break
		}

		binary.Read(bytes.NewReader(buf), binary.LittleEndian, &fileNumber)

		fd.Read(buf)
		binary.Read(bytes.NewReader(buf), binary.LittleEndian, &expireAt)

		fd.Read(buf)
		binary.Read(bytes.NewReader(buf), binary.LittleEndian, &valuePos)

		fd.Read(buf)
		binary.Read(bytes.NewReader(buf), binary.LittleEndian, &valueLen)

		fd.Read(buf)
		binary.Read(bytes.NewReader(buf), binary.LittleEndian, &keyLen)

		keyBuf := make([]byte, keyLen)
		fd.Read(keyBuf)

		writeHashMap(db, fileNumber, expireAt, valuePos, valueLen, string(keyBuf))
	}

	return nil
}

func writeHashMap(db *Db, fileNumber,expireAt,valuePos,valueLen int64, key string) {
	db.Hashmap[0].Lock.Lock()
	defer db.Hashmap[0].Lock.Unlock()
	hb := &HashBlock{
		FileNumber: int(fileNumber),
		ValuePos: valuePos,
		ValueLen: int(valueLen),
		ExpireAt: int(expireAt),
	}
	db.Hashmap[0].Data[key] = hb
}

func DeleteHintFile() {
	lockFilePath := config.Db.DataDir + "hint.lock"
	hintFilePath := config.Db.DataDir + "hint"
	os.Remove(lockFilePath)
	os.Remove(hintFilePath)
}