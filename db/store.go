package db

import (
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
}

type Store struct {
	ActiveFileNumber int    //当前文件编号
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

func (self *Store) Add(key string, value string, expireAt int) (int ,int64, error) {
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

	buffer := bytes.NewBuffer([]byte{})
	binary.Write(buffer, binary.BigEndian, int64(fb.Crc))
	binary.Write(buffer, binary.BigEndian, int64(fb.ExpireAt))
	binary.Write(buffer, binary.BigEndian, int64(fb.KeyLen))
	binary.Write(buffer, binary.BigEndian, int64(fb.ValueLen))

	buffer.Write([]byte(fb.Key))
	buffer.Write([]byte(fb.Value))

	fd, err := os.OpenFile(self.getActiveFilePath(), os.O_CREATE | os.O_WRONLY | os.O_APPEND, 0644)
	if err != nil {
		return 0, 0, err
	}
	defer fd.Close()

	self.FileLock.Lock()
	defer self.FileLock.Unlock()
	currOffset,err := fd.Seek(0, os.SEEK_END)
	_,err = fd.Write(buffer.Bytes())
	if err != nil {
		return 0, 0, err
	}
	offset := currOffset +
		int64(unsafe.Sizeof(int64(fb.Crc))) +
		int64(unsafe.Sizeof(int64(fb.ExpireAt))) +
		int64(unsafe.Sizeof(int64(fb.KeyLen))) +
		int64(unsafe.Sizeof(int64(fb.ValueLen))) +
		int64(len(fb.Key))

	return self.ActiveFileNumber, offset, nil
}

func (self *Store) GetValue(fileNumber int, pos int64, len int) (string, error){
	filePath := self.getFilePath(fileNumber)
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

func (self *Store) getActiveFilePath() string {
	return self.getFilePath(self.ActiveFileNumber)
}

func (self *Store) getFilePath(fileNumber int) string {
	return fmt.Sprintf("%sdb.%d",self.DB.DataDir, fileNumber)
}

func (self *Store) CalculationActiveFileNumber() int{
	matches,_ := filepath.Glob(self.DB.DataDir + "/db.*");
	nums := []int{}
	for _,v := range matches {
		index := strings.LastIndex(v,"db.")
		num,_ := strconv.Atoi(string(v[index+3:]))
		nums = append(nums, num)
	}
	if len(nums) == 0 {
		return 0
	}
	sort.Ints(nums)
	return nums[len(nums)-1]
}
