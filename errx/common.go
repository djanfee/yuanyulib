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
	ErrServer = New(ErrServerCode, "系统错误")
	ErrDB     = New(ErrDBCode, "存储错误")
	ErrCache  = New(ErrCacheCode, "缓存错误")

	// ***通用错误***
	ErrParam          = New(ErrParamCode, "参数错误")
	ErrRecordNotFound = New(ErrRecordNotFoundCode, "数据不存在")
	ErrRecordExisted  = New(ErrRecordExistedCode, "数据已存在")
	ErrUnauthorized   = New(ErrUnauthorizedCode, "无权限")
)
