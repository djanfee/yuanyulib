package validator

import (
	"sync"

	"github.com/djanfee/yuanyulib/errx"
	"github.com/go-playground/locales/zh"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	zhtranslations "github.com/go-playground/validator/v10/translations/zh"
)

var validate *validator.Validate
var trans ut.Translator
var once sync.Once

func Init() {
	once.Do(func() {
		if validate == nil {
			validate = validator.New()
			// 初始化中文翻译
			zh := zh.New()
			uni := ut.New(zh, zh)
			trans, _ = uni.GetTranslator("zh")
			// 注册中文翻译
			_ = zhtranslations.RegisterDefaultTranslations(validate, trans)
		}
		registerTags()
	})
}

// Validate 验证
func Validate(req interface{}) error {
	if err := validate.Struct(req); err != nil {
		for _, e := range err.(validator.ValidationErrors) {
			return errx.NewParamErr(e.Translate(trans))
		}
	}
	return nil
}
