package tool

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
)

func GzipEncode(data []byte) []byte {
	buf := bytes.NewBuffer([]byte{})
	w := gzip.NewWriter(buf)
	defer w.Close()
	w.Write(data)
	w.Flush()
	return buf.Bytes()
}

func GzipDecode(data []byte) []byte {
	r,_ := gzip.NewReader(bytes.NewReader(data))
	defer r.Close()

	//buf := make([]byte,len(data))
	//r.Read(buf)
	buf,_ := ioutil.ReadAll(r)
	return buf
}

func CopyFile(dstName, srcName string) (written int64, err error) {
	src, err := os.Open(srcName)
	if err != nil {
		return
	}
	defer src.Close()
	dst, err := os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer dst.Close()
	return io.Copy(dst, src)
}