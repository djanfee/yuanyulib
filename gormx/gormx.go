package gormx

import (
	"fmt"
	"sync"

	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type DBManager struct {
	sync.RWMutex
	Mysql      *gorm.DB
	Postgres   *gorm.DB
	ClickHouse *gorm.DB
}

func NewDBManager() *DBManager {
	return &DBManager{}
}

func (dm *DBManager) NewEngine(c Config) error {
	var (
		dialector gorm.Dialector
		dbKey     string
	)

	switch c.Mode {
	case Mysql:
		dialector = mysql.Open(c.DSN)
		dbKey = "Mysql"
	case Postgres:
		dialector = postgres.Open(c.DSN)
		//dialector = postgres.New(postgres.Config{
		//	DSN:                  c.DSN,
		//	PreferSimpleProtocol: true, // disables implicit prepared statement usage
		//})
		dbKey = "Postgres"
	case ClickHouse:
		dialector = clickhouse.Open(c.DSN)
		dbKey = "ClickHouse"
	default:
		return fmt.Errorf("unsupported database mode: %d", c.Mode)
	}

	engine, err := NewEngine(c, dialector)
	if err != nil {
		return fmt.Errorf("failed to initialize %s database: %v", dbKey, err)
	}

	dm.Lock()
	defer dm.Unlock()

	switch c.Mode {
	case Mysql:
		if dm.Mysql == nil {
			dm.Mysql = engine
		} else {
			return fmt.Errorf("mysql connection already exists")
		}
	case Postgres:
		if dm.Postgres == nil {
			dm.Postgres = engine
		} else {
			return fmt.Errorf("postgres connection already exists")
		}
	case ClickHouse:
		if dm.ClickHouse == nil {
			dm.ClickHouse = engine
		} else {
			return fmt.Errorf("clickhouse connection already exists")
		}
	}

	return nil
}

// Initialize initialize the database
func (dm *DBManager) Initialize(cs ...Config) error {
	for _, c := range cs {
		if err := dm.NewEngine(c); err != nil {
			return err
		}
	}
	return nil
}

// CloseAll close all connections
func (dm *DBManager) CloseAll() error {
	var errs []error
	dm.Lock()
	defer dm.Unlock()

	closeDB := func(dbName string, db *gorm.DB) {
		if db != nil {
			sqlDB, err := db.DB()
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to get underlying sql.DB for %s: %v", dbName, err))
				return
			}

			if e := sqlDB.Close(); e != nil {
				errs = append(errs, fmt.Errorf("failed to close %s connection: %v", dbName, e))
			}
		}
	}

	closeDB("MySQL", dm.Mysql)
	closeDB("Postgres", dm.Postgres)
	closeDB("ClickHouse", dm.ClickHouse)

	if len(errs) > 0 {
		return fmt.Errorf("errors occurred while closing connections: %v", errs)
	}
	return nil
}
