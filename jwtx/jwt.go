package jwtx

import (
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

var (
	ErrTokenExpired     = errors.New("token已过期")
	ErrTokenInvalid     = errors.New("token无效")
	ErrTokenMalformed   = errors.New("token格式错误")
	ErrTokenNotValidYet = errors.New("token尚未生效")
)

// Claims JWT载荷
type Claims struct {
	UID        string `json:"uid"`
	Username   string `json:"username"`
	MerchantId string `json:"merchant_id"`
	Currency   string `json:"currency"`

	jwt.RegisteredClaims
}

// JWTUtil JWT工具
type JWTUtil struct {
	Secret        []byte
	AccessExpire  int64 // 访问令牌过期时间（秒）
	RefreshExpire int64 // 刷新令牌过期时间（秒）
}

// NewJWTUtil 创建JWT工具实例
func NewJWTUtil(secret string, accessExpire, refreshExpire int64) *JWTUtil {
	return &JWTUtil{
		Secret:        []byte(secret),
		AccessExpire:  accessExpire,
		RefreshExpire: refreshExpire,
	}
}

// GenerateToken 生成访问令牌
func (j *JWTUtil) GenerateToken(uid, username, merchantId, currency string) (string, int64, error) {
	now := time.Now()
	expireAt := now.Add(time.Duration(j.AccessExpire) * time.Second)

	claims := Claims{
		UID:        uid,
		Username:   username,
		MerchantId: merchantId,
		Currency:   currency,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expireAt),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(j.Secret)
	if err != nil {
		return "", 0, err
	}

	return tokenString, expireAt.Unix(), nil
}

// GenerateRefreshToken 生成刷新令牌（有效期为访问令牌的2倍）
func (j *JWTUtil) GenerateRefreshToken(uid string, username, merchantId, currency string) (string, int64, error) {
	now := time.Now()
	expireAt := now.Add(time.Duration(j.RefreshExpire) * time.Second)

	claims := Claims{
		UID:        uid,
		Username:   username,
		MerchantId: merchantId,
		Currency:   currency,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expireAt),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(j.Secret)
	if err != nil {
		return "", 0, err
	}

	return tokenString, expireAt.Unix(), nil
}

// ParseToken 解析token
func (j *JWTUtil) ParseToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return j.Secret, nil
	})

	if err != nil {
		if ve, ok := err.(*jwt.ValidationError); ok {
			if ve.Errors&jwt.ValidationErrorMalformed != 0 {
				return nil, ErrTokenMalformed
			} else if ve.Errors&jwt.ValidationErrorExpired != 0 {
				return nil, ErrTokenExpired
			} else if ve.Errors&jwt.ValidationErrorNotValidYet != 0 {
				return nil, ErrTokenNotValidYet
			}
		}
		return nil, ErrTokenInvalid
	}

	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims, nil
	}

	return nil, ErrTokenInvalid
}

// RefreshToken 刷新token（使用刷新令牌换取新的访问令牌）
func (j *JWTUtil) RefreshToken(refreshToken string) (string, int64, error) {
	claims, err := j.ParseToken(refreshToken)
	if err != nil {
		return "", 0, err
	}

	// 生成新的访问令牌
	return j.GenerateToken(claims.UID, claims.Username, claims.MerchantId, claims.Currency)
}
