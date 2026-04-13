package errx

type ErrCode int

const (
	ErrServerCode ErrCode = 1000001 // 系统错误
	ErrDBCode     ErrCode = 1000002 // 数据库错误
	ErrCacheCode  ErrCode = 1000003 // 缓存错误

	ErrParamCode          ErrCode = 1001001 // 参数错误
	ErrRecordNotFoundCode ErrCode = 1001002 // 数据不存在
	ErrRecordExistedCode  ErrCode = 1001003 // 数据已存在
	ErrUnauthorizedCode   ErrCode = 1001004 // 无权限
)

var (
	// ***系统错误**
	ErrServer = New(ErrServerCode, "server error")
	ErrDB     = New(ErrDBCode, "storage error")
	ErrCache  = New(ErrCacheCode, "cache error")

	// ***通用错误***
	ErrParam          = New(ErrParamCode, "params error")
	ErrRecordNotFound = New(ErrRecordNotFoundCode, "record not found")
	ErrRecordExisted  = New(ErrRecordExistedCode, "record existed")
	ErrUnauthorized   = New(ErrUnauthorizedCode, "unauthorized")
)
