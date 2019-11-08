package db

import (
	"bcdb/config"
	"bcdb/tool"
	bufio2 "bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

//文件块
type FileBlock struct {
	Crc  uint32 //效验码
	ExpireAt int    //到期时间戳 秒
	KeyLen    int    //key长度
	ValueLen  int    //value长度
	Key       string //key数据
	Value     string //值

	FileNumber int  //文件编号 -1为快照
	ValuePos   int64
}



type Store struct {
	ActiveFileNumber int    //当前文件编号
	ActiveFileNumberChangeLock sync.Mutex //改变文件编号的锁
	DB *Db //当前的db
	FileLock sync.Mutex //写文件锁
}

func NewStore(db *Db) *Store {
	store := &Store{
		DB: db,
	}
	store.ActiveFileNumber = store.CalculationActiveFileNumber()
	return store
}

func (self *Store) updateActiveFileNumber() {
	if self.DB.IsRunMerge == true {
		return
	}

	if len(self.GetDataDirFileNumbers()) > 10 {
		go func() {
			self.DB.merge.Run(self.GetDataDirFileNumbers())
		}()

		oldFileNumber := self.ActiveFileNumber
		self.ActiveFileNumberChangeLock.Lock()
		defer self.ActiveFileNumberChangeLock.Unlock()

		if oldFileNumber == self.ActiveFileNumber {
			self.ActiveFileNumber++
		}

		return
	}

	fd,err := os.OpenFile(self.getActiveFilePath(), os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer fd.Close()

	fileInfo, err := fd.Stat()
	if err != nil {
		return
	}

	if fileInfo.Size() > config.Db.MaxFileSize * 1000 {
		oldFileNumber := self.ActiveFileNumber

		self.ActiveFileNumberChangeLock.Lock()
		defer self.ActiveFileNumberChangeLock.Unlock()

		if oldFileNumber == self.ActiveFileNumber {
			self.ActiveFileNumber++
		}
	}
}

func (self *Store) Add(key string, value string, expireAt int) (*FileBlock, error) {
	self.updateActiveFileNumber()

	fb := self.GetStoreFileBlock(key, value, expireAt)

	fileStoreBytes := self.GetFileStoreBytes(fb)

	fd, err := os.OpenFile(self.getActiveFilePath(), os.O_CREATE | os.O_WRONLY | os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	self.FileLock.Lock()
	defer self.FileLock.Unlock()
	currOffset,err := fd.Seek(0, os.SEEK_END)
	_,err = fd.Write(fileStoreBytes)
	if err != nil {
		return nil, err
	}
	offset := currOffset +
		int64(unsafe.Sizeof(int64(fb.Crc))) +
		int64(unsafe.Sizeof(int64(fb.ExpireAt))) +
		int64(unsafe.Sizeof(int64(fb.KeyLen))) +
		int64(unsafe.Sizeof(int64(fb.ValueLen))) +
		int64(len(fb.Key))

	fb.FileNumber = self.ActiveFileNumber
	fb.ValuePos = offset
	return fb, nil
}

func (self *Store) GetValue(fileNumber int, pos int64, len int) (string, error){
	filePath := self.GetFilePath(fileNumber)
	fd,err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer fd.Close()
	fd.Seek(pos, os.SEEK_CUR)

	r := bufio2.NewReader(fd)
	buf := make([]byte, len)
	r.Read(buf)

	buf = tool.GzipDecode(buf)

	return string(buf), nil
}

func (self *Store) GetStoreFileBlock(key, value string, expireAt int) *FileBlock {
	key = string(tool.GzipEncode([]byte(key)))
	value = string(tool.GzipEncode([]byte(value)))

	fb := &FileBlock{
		ExpireAt: expireAt,
		KeyLen: len(key),
		ValueLen: len(value),
		Key: key,
		Value: value,
	}
	crcString := fmt.Sprintf("%d%d%d%s%s",
		fb.ExpireAt,
		fb.KeyLen,
		fb.ValueLen,
		fb.Key,
		fb.Value,
	)
	fb.Crc = crc32.ChecksumIEEE([]byte(crcString))
	return fb
}

func (self *Store) GetFileStoreBytes(fb *FileBlock) ([]byte){

	buffer := bytes.NewBuffer([]byte{})
	binary.Write(buffer, binary.BigEndian, int64(fb.Crc))
	binary.Write(buffer, binary.BigEndian, int64(fb.ExpireAt))
	binary.Write(buffer, binary.BigEndian, int64(fb.KeyLen))
	binary.Write(buffer, binary.BigEndian, int64(fb.ValueLen))

	buffer.Write([]byte(fb.Key))
	buffer.Write([]byte(fb.Value))

	return buffer.Bytes()
}

func (self *Store) getActiveFilePath() string {
	return self.GetFilePath(self.ActiveFileNumber)
}

func (self *Store) GetFilePath(fileNumber int) string {
	if fileNumber == -1 {
		return fmt.Sprintf("%ssdb",config.Db.DataDir)
	}
	return fmt.Sprintf("%sdb.%d",config.Db.DataDir, fileNumber)
}

func (self *Store) CalculationActiveFileNumber() int{
	nums := self.GetDataDirFileNumbers()
	if len(nums) == 0 {
		return 0
	}
	sort.Ints(nums)
	return nums[len(nums)-1]
}

func (self *Store) GetDataDirFileNumbers() []int {
	matches,_ := filepath.Glob(config.Db.DataDir + "/db.*");
	nums := []int{}
	for _,v := range matches {
		index := strings.LastIndex(v,"db.")
		num,_ := strconv.Atoi(string(v[index+3:]))
		nums = append(nums, num)
	}
	if len(nums) > 0 {
		sort.Ints(nums)
	}

	return nums
}

func (self *Store) LoadAllFileData(in chan<- FileBlock) {
	nums := self.GetDataDirFileNumbers()
	for _,num := range nums {
		self.LoadFileData(num, in)
	}
	close(in)
}

func (self *Store) LoadFileData(fileNum int, in chan<- FileBlock) {
	filepath := self.GetFilePath(fileNum)
	fd, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer fd.Close()

	for {
		fb := FileBlock{}
		fb.FileNumber = fileNum

		buf := make([]byte, unsafe.Sizeof(int64(fb.Crc)))
		_, err := fd.Read(buf)
		if err != nil {
			break
		}
		var crc int64
		var expireAt int64
		var keyLen int64
		var valueLen int64
		binary.Read(bytes.NewReader(buf), binary.BigEndian, &crc)

		fd.Read(buf)
		binary.Read(bytes.NewReader(buf), binary.BigEndian, &expireAt)

		fd.Read(buf)
		binary.Read(bytes.NewReader(buf), binary.BigEndian, &keyLen)

		fd.Read(buf)
		binary.Read(bytes.NewReader(buf), binary.BigEndian, &valueLen)

		keyBuf := make([]byte,keyLen)
		fd.Read(keyBuf)
		keyDecodeBuf := tool.GzipDecode(keyBuf)

		fb.ValuePos,_ = fd.Seek(0, os.SEEK_CUR)
		valueBuf := make([]byte, valueLen)
		fd.Read(valueBuf)
		valueDecodeBuf := tool.GzipDecode(valueBuf)

		fb.Crc = uint32(crc)
		fb.ExpireAt = int(expireAt)
		fb.KeyLen = int(keyLen)
		fb.ValueLen = int(valueLen)
		fb.Key = string(keyDecodeBuf)
		fb.Value = string(valueDecodeBuf)

		in <- fb

	}

}
