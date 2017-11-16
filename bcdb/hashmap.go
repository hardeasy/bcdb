package bcdb

//hash存储的块
type HashBlock struct {
	FileNumber int   //文件编号
	ValuePos   int64 //值的位置int64
	ValueLen   int   //值的长度
	Timestamp  int   //到期时间戳 秒
}

type HashMap struct {
	Data map[string]*HashBlock
}

func (self *HashMap) Add(key string, valueLen int, timestamp int) *HashBlock {
	hb := &HashBlock{
		FileNumber: 0,
		ValuePos:   0,
		ValueLen:   valueLen,
		Timestamp:  timestamp,
	}
	self.Data[key] = hb

	return hb
}

func (self *HashMap) Get(key string) *HashBlock {
	hb, ok := self.Data[key]
	if !ok {
		return nil
	}
	return hb
}
