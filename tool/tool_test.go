package tool

import (
	"testing"
)

func TestGzipEncodeAndDecode(t *testing.T) {
	rawData := []byte(`hello, 中新经纬客户端11月6日电 据国家发改委网站6日消息，10月30日，国家发展改革委修订发布了《产业结构调整指导目录(2019年本)》(以下简称《目录(2019年本)》)。国家发改委产业发展司负责人就《目录(2019年本)》答记者问时表示，此次修订重点包含破除无效供给、推动制造业高质量发展等四方面。鼓励类新增“人力资源与人力资本服务业”、“人工智能”、“养老与托育服务”、“家政”等4个行业。`)
	encodeData := GzipEncode(rawData)
	decodeData := GzipDecode(encodeData)

	if string(rawData) != string(decodeData) {
		t.Fail()
	}
	t.Log("raw len: ", len(rawData), "encde len: ", len(encodeData))
}