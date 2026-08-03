package encryptx

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

// MD5 MD5加密
func MD5(data string) string {
	temp := md5.Sum([]byte(data))
	return hex.EncodeToString(temp[:])
}

func Encrypt(source string) string {
	source = strings.TrimSpace(source)
	data := source + "bcrypt.GenerateFromPassword([]byte(source), bcrypt.DefaultCost)"
	temp := md5.Sum([]byte(data))
	return hex.EncodeToString(temp[:])
}

// SortJsonBytes 解析 JSON 对象，按字段名字符串排序后拼接为紧凑 JSON 字符串。
func SortJsonBytes(body []byte) (string, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return "", nil
	}

	var data map[string]json.RawMessage
	if err := json.Unmarshal(body, &data); err != nil {
		return "", err
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, strings.TrimSpace(k))
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		keyBytes, err := json.Marshal(k)
		if err != nil {
			return "", err
		}
		sb.Write(keyBytes)
		sb.WriteByte(':')
		sb.Write(bytes.TrimSpace(data[k]))
	}
	sb.WriteByte('}')
	return sb.String(), nil
}
