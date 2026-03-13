package gormx

type DbMode int

const (
	Mysql DbMode = iota + 1
	Postgres
	ClickHouse
)

type Config struct {
	Mode                   DbMode   `json:"mode"`
	Separation             bool     `json:",default=false"`
	Trace                  bool     `json:"trace,default=false"`
	Master                 string   `json:"master,optional"`
	Sources                []string `json:"sources,optional"`
	Replicas               []string `json:"replicas,optional"`
	DSN                    string   `json:"dsn,optional"`
	Debug                  bool     `json:"debug,default=false"`
	MaxIdleConn            int      `json:"max_idle_conn"`
	MaxOpenConn            int      `json:"max_open_conn"`
	MaxLifetime            int      `json:"max_lifetime"`
	PrepareStmt            bool     `json:"prepare_stmt"`
	SkipDefaultTransaction bool     `json:"skip_default_transaction"`
}
