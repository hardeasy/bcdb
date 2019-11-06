package tool

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
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
