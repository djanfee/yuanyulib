package errx

import (
	"fmt"

	"github.com/djanfee/yuanyulib/utils/xhttp"
	"github.com/zeromicro/x/errors"
	"google.golang.org/grpc/status"
)

// New 创建自定义错误
func New(code ErrCode, msg string, args ...interface{}) error {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	return errors.New(int(code), msg)
}

func NewParamErr(msg string) error {
	return New(ErrParamCode, msg)
}

func NewRpcErr(err error) error {
	if s, ok := status.FromError(err); ok {
		return New(ErrCode(s.Code()), s.Message())
	}
	return err
}

// ErrMsg 业务错误
type ErrMsg string

const (
	Success ErrMsg = "Success" // 正常
)

func (e ErrMsg) Str() string {
	return string(e)
}

// NewWithResult 创建自定义带返回的错误
func NewWithResult(code ErrCode, msg string, data any) error {
	return xhttp.New(int(code), msg, data)
}
