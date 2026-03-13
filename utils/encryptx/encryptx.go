package encryptx

import (
	"crypto/md5"
	"encoding/hex"
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
