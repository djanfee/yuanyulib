package compressx

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"io"
)

// GzipCompress 将数据进行gzip压缩，适合存储到字符串字段
func GzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	defer w.Close()
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GzipDecompress 将gzip数据解压
func GzipDecompress(compressed []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// ParseSceneData 解析 scene 数据，支持压缩格式 {"data":"<base64 gzip>"} 和普通 JSON 格式
// 数据库 bytea 列通过 JSON 返回时会变成 PostgreSQL hex 格式的字符串: "\x1f8b08..."
func ParseSceneData(raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return nil
	}
	// raw 是 json.RawMessage，可能是 JSON 字符串 "\\x1f8b08..." 或 JSON 数组 [...]
	// 先尝试作为 JSON 字符串解析（处理 PostgreSQL bytea hex 格式）
	var hexStr string
	if err := json.Unmarshal(raw, &hexStr); err == nil && len(hexStr) > 2 && hexStr[:2] == "\\x" {
		// PostgreSQL bytea hex 格式: \x1f8b08...
		binData, err := hex.DecodeString(hexStr[2:])
		if err == nil {
			decompressed, err := GzipDecompress(binData)
			if err == nil {
				return decompressed
			}
		}
	}

	// 尝试直接 gzip 解压（二进制格式）
	decompressed, err := GzipDecompress(raw)
	if err == nil {
		return decompressed
	}

	// 降级：直接解析普通 JSON 格式
	return raw
}
