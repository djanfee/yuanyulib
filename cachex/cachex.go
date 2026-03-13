package cachex

import (
	"sync"

	"github.com/dtm-labs/rockscache"
	"github.com/redis/go-redis/v9"
)

var Engine *rockscache.Client
var once sync.Once

func Must(c Config, rdb redis.UniversalClient) {
	once.Do(func() {
		if Engine == nil {
			Engine = NewEngine(c, rdb)
		}
	})
}

// NewEngine 创建一个缓存引擎
func NewEngine(c Config, rdb redis.UniversalClient) *rockscache.Client {
	options := rockscache.NewDefaultOptions()
	options.StrongConsistency = c.StrongConsistency
	options.DisableCacheRead = c.DisableCacheRead
	return rockscache.NewClient(rdb, options)
}
