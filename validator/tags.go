package validator

import (
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/shopspring/decimal"
)

func registerTags() {
	_ = validate.RegisterValidation("alpha_num", alphaNum)
	_ = validate.RegisterValidation("nickname", nickname)
	_ = validate.RegisterValidation("pwd", pwd)
	_ = validate.RegisterValidation("not_empty", notEmpty)
	_ = validate.RegisterValidation("no_special", noSpecial)
	_ = validate.RegisterValidation("ip", ip)
	_ = validate.RegisterValidation("num_str_gt", numStrGreaterThan)
	_ = validate.RegisterValidation("num_str_gte", numStrGreaterThanOrEqual)
	_ = validate.RegisterValidation("num_str_lt", numStrLessThan)
	_ = validate.RegisterValidation("num_str_lte", numStrLessThanOrEqual)
	_ = validate.RegisterValidation("two_decimal_places", float64WithTwoDecimalPlaces)
	_ = validate.RegisterValidation("not_chinese", notChinese)
}

// 英文字母加数字
func alphaNum(fl validator.FieldLevel) bool {
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return false
	}

	match, _ := regexp.MatchString("^[a-zA-Z0-9]+$", s)

	return match
}

// 昵称，中英文数字组合
func nickname(fl validator.FieldLevel) bool {
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return false
	}

	match, _ := regexp.MatchString("^[a-zA-Z0-9\u4e00-\u9fa5]+$", s)
	return match
}

// 密码，字母/符号/数字 的随机组合
func pwd(fl validator.FieldLevel) bool {
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return false
	}

	match, _ := regexp.MatchString("^[a-zA-Z0-9\\W_]+$", s)
	return match
}

// 数组不能为空
func notEmpty(fl validator.FieldLevel) bool {
	field := fl.Field()
	//
	switch field.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map, reflect.String:
		return field.Len() > 0
	// 其他
	default:
		return false
	}
}

// 不能包含特殊字符
func noSpecial(fl validator.FieldLevel) bool {
	value := fl.Field().String()
	for _, char := range value {
		if char == '!' || char == '@' {
			return false
		}
	}
	return true
}

func ip(fl validator.FieldLevel) bool {
	s, ok := fl.Field().Interface().(string)
	if !ok {
		return false
	}
	if len(s) == 0 {
		return true
	}

	return net.ParseIP(s) != nil
}

func numStrGreaterThan(fl validator.FieldLevel) bool {

	fieldNum, err := decimal.NewFromString(fl.Field().String())
	if err != nil {
		return false
	}

	valueNum, err := decimal.NewFromString(fl.Param())
	if err != nil {
		return false
	}
	return fieldNum.GreaterThan(valueNum)
}

func numStrGreaterThanOrEqual(fl validator.FieldLevel) bool {

	fieldNum, err := decimal.NewFromString(fl.Field().String())
	if err != nil {
		return false
	}

	valueNum, err := decimal.NewFromString(fl.Param())
	if err != nil {
		return false
	}
	return fieldNum.GreaterThanOrEqual(valueNum)
}

func numStrLessThan(fl validator.FieldLevel) bool {

	fieldNum, err := decimal.NewFromString(fl.Field().String())
	if err != nil {
		return false
	}

	valueNum, err := decimal.NewFromString(fl.Param())
	if err != nil {
		return false
	}
	return fieldNum.LessThan(valueNum)
}

func numStrLessThanOrEqual(fl validator.FieldLevel) bool {

	fieldNum, err := decimal.NewFromString(fl.Field().String())
	if err != nil {
		return false
	}

	valueNum, err := decimal.NewFromString(fl.Param())
	if err != nil {
		return false
	}
	return fieldNum.LessThanOrEqual(valueNum)
}

// float64WithTwoDecimalPlaces float64保留2位小数
func float64WithTwoDecimalPlaces(fl validator.FieldLevel) bool {
	value, ok := fl.Field().Interface().(float64)
	if !ok {
		return false
	}
	parts := strings.Split(strconv.FormatFloat(value, 'f', -1, 64), ".")
	if len(parts) == 2 && len(parts[1]) > 2 {
		return false
	}

	return true
}

// 不包含中文字符
func notChinese(fl validator.FieldLevel) bool {
	value, ok := fl.Field().Interface().(string)
	if !ok {
		return false
	}
	pattern := `^[^\x{4E00}-\x{9FFF}]*$`
	return regexp.MustCompile(pattern).MatchString(value)
}
