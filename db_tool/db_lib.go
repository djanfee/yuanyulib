package db_tool

import (
	"crypto/aes"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// ParamDef 参数定义
type ParamDef struct {
	Name      string `xml:"Name,attr"`
	Type      string `xml:"Type,attr"`
	MaxLength int    `xml:"MaxLength,attr,omitempty"`
}

// FieldDef 输出字段定义
type FieldDef struct {
	Name      string `xml:"Name,attr"`
	Type      string `xml:"Type,attr"`
	MaxLength int    `xml:"MaxLength,attr,omitempty"`
}

// OutputRecord 输出记录结构定义
type OutputRecord struct {
	Fields []FieldDef `xml:"Field"`
}

// RequestDef 存储过程请求定义
type RequestDef struct {
	ID              int           `xml:"ID,attr"`
	ClusterID       int           `xml:"ClusterID,attr"`
	StoredProcedure string        `xml:"StoredProcedure,attr"`
	InputParams     []ParamDef    `xml:"InputParam"`
	OutputParams    []ParamDef    `xml:"OutputParam"`
	OutputRecord    *OutputRecord `xml:"OutputRecord"`
}

// DbSystemConfig 请求配置（ReqCfg）
type DbSystemConfig struct {
	Requests []RequestDef `xml:"Request"`
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	ID             int    `xml:"ID,attr"`
	UserID         string `xml:"UserID,attr"`
	Password       string `xml:"Password,attr"`
	DataSource     string `xml:"DataSource,attr"`
	InitialCatalog string `xml:"InitialCatalog,attr"`
	Schema         string `xml:"Schema,attr"`
}

// ConnectionConfig 连接配置
type ConnectionConfig struct {
	PacketSize int `xml:"PacketSize,attr"`
}

// ConnectionPoolConfig 连接池配置
type ConnectionPoolConfig struct {
	Max int `xml:"Max,attr"`
}

// ClusterConfig 集群配置
type ClusterConfig struct {
	Database       DatabaseConfig       `xml:"Database"`
	Connection     ConnectionConfig     `xml:"Connection"`
	ConnectionPool ConnectionPoolConfig `xml:"ConnectionPool"`
}

// DbCfgSystem 数据库配置系统（DbCfg）
type DbCfgSystem struct {
	Clusters []ClusterConfig `xml:"Cluster"`
}

// DataBaseTool 数据库操作工具
type DataBaseTool struct {
	dbs            map[int]*gorm.DB        // ClusterID -> 数据库连接映射
	dbConfigs      map[int]*DatabaseConfig // ClusterID -> 数据库配置映射
	requests       map[int]*RequestDef     // 以ID为key的请求定义映射
	requestsByName map[string]*RequestDef  // 以存储过程名称为key的请求定义映射
	aesKey         string
	dbCfgFilePath  string
	reqCfgFilePath string
	dbCfgModTime   time.Time    // DbCfg 文件修改时间
	reqCfgModTime  time.Time    // ReqCfg 文件修改时间
	mu             sync.RWMutex // 读写锁，保护配置更新
	stopMonitor    chan struct{}
}

// NewDataBaseTool 创建数据库操作工具（支持多数据库连接）
// aesKey: AES密钥
// dbCfgFilePath: 加密的数据库配置文件路径（DbCfg2.res）
// reqCfgFilePath: 加密的请求配置文件路径（ReqCfg2.res）
func NewDataBaseTool(aesKey string, dbCfgFilePath string, reqCfgFilePath string) (*DataBaseTool, error) {
	// 1. 解密数据库配置文件
	dbCfgContent, err := decryptConfigFile(aesKey, dbCfgFilePath)
	if err != nil {
		return nil, fmt.Errorf("解密数据库配置文件失败: %w", err)
	}

	// 2. 解密请求配置文件
	reqCfgContent, err := decryptConfigFile(aesKey, reqCfgFilePath)
	if err != nil {
		return nil, fmt.Errorf("解密请求配置文件失败: %w", err)
	}

	return newDataBaseToolFromContent(dbCfgContent, reqCfgContent, dbCfgFilePath, reqCfgFilePath, aesKey)
}

// NewDataBaseToolFromXML 从未加密的XML文件创建数据库操作工具（支持多数据库连接）
// dbCfgFilePath: 数据库配置XML文件路径（DbCfg2.xml）
// reqCfgFilePath: 请求配置XML文件路径（ReqCfg2.xml）
func NewDataBaseToolFromXML(dbCfgFilePath string, reqCfgFilePath string) (*DataBaseTool, error) {
	// 1. 读取数据库配置XML文件
	dbCfgContent, err := ioutil.ReadFile(dbCfgFilePath)
	if err != nil {
		return nil, fmt.Errorf("读取数据库配置XML文件失败: %w", err)
	}

	// 2. 读取请求配置XML文件
	reqCfgContent, err := ioutil.ReadFile(reqCfgFilePath)
	if err != nil {
		return nil, fmt.Errorf("读取请求配置XML文件失败: %w", err)
	}

	return newDataBaseToolFromContent(string(dbCfgContent), string(reqCfgContent), dbCfgFilePath, reqCfgFilePath, "")
}

// newDataBaseToolFromContent 从XML内容创建数据库操作工具（内部函数）
func newDataBaseToolFromContent(dbCfgContent string, reqCfgContent string, dbCfgFilePath string, reqCfgFilePath string, aesKey string) (*DataBaseTool, error) {
	// 1. 解析数据库配置XML
	var dbCfg DbCfgSystem
	err := xml.Unmarshal([]byte(dbCfgContent), &dbCfg)
	if err != nil {
		return nil, fmt.Errorf("解析数据库配置XML失败: %w", err)
	}

	// 2. 解析请求配置XML
	var reqCfg DbSystemConfig
	err = xml.Unmarshal([]byte(reqCfgContent), &reqCfg)
	if err != nil {
		return nil, fmt.Errorf("解析请求配置XML失败: %w", err)
	}

	// 3. 建立数据库连接
	dbs := make(map[int]*gorm.DB)
	dbConfigs := make(map[int]*DatabaseConfig)

	for i := range dbCfg.Clusters {
		cluster := &dbCfg.Clusters[i]
		dbConfig := &cluster.Database

		// 解析 DataSource (格式: IP:Port)
		host := dbConfig.DataSource
		port := "5432" // 默认 PostgreSQL 端口
		if idx := strings.Index(dbConfig.DataSource, ":"); idx != -1 {
			host = dbConfig.DataSource[:idx]
			port = dbConfig.DataSource[idx+1:]
		}

		// 构建 PostgreSQL DSN
		dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			host,
			port,
			dbConfig.UserID,
			dbConfig.Password,
			dbConfig.InitialCatalog,
		)

		// 配置 PostgreSQL 驱动
		// 使用 PreferSimpleProtocol 来避免 prepared statement 缓存冲突（兼容 PgBouncer）
		pgConfig := postgres.Config{
			DSN:                  dsn,
			PreferSimpleProtocol: true, // 使用简单协议，不使用 prepared statements
		}
		dialector := postgres.New(pgConfig)

		// 连接数据库
		db, err := gorm.Open(dialector, &gorm.Config{
			PrepareStmt:            false, // 禁用 prepared statement 缓存
			SkipDefaultTransaction: true,  // 提升性能
		})
		if err != nil {
			return nil, fmt.Errorf("连接数据库失败 (ClusterID=%d): %w", dbConfig.ID, err)
		}

		// 配置连接池
		sqlDB, err := db.DB()
		if err != nil {
			return nil, fmt.Errorf("获取底层数据库连接失败 (ClusterID=%d): %w", dbConfig.ID, err)
		}

		// 设置连接池参数（从配置文件读取或使用默认值）
		maxOpenConns := cluster.ConnectionPool.Max
		if maxOpenConns <= 0 {
			maxOpenConns = 10 // 默认值
		}
		sqlDB.SetMaxOpenConns(maxOpenConns)
		sqlDB.SetMaxIdleConns(maxOpenConns / 2) // 空闲连接数为最大连接数的一半
		sqlDB.SetConnMaxLifetime(time.Hour)

		dbs[dbConfig.ID] = db
		dbConfigs[dbConfig.ID] = dbConfig
	}

	// 4. 构建请求映射
	requests := make(map[int]*RequestDef)
	requestsByName := make(map[string]*RequestDef)
	for i := range reqCfg.Requests {
		req := &reqCfg.Requests[i]
		requests[req.ID] = req
		requestsByName[req.StoredProcedure] = req
	}

	// 5. 获取文件修改时间
	dbCfgFileInfo, err := os.Stat(dbCfgFilePath)
	if err != nil {
		return nil, fmt.Errorf("获取DbCfg文件信息失败: %w", err)
	}
	reqCfgFileInfo, err := os.Stat(reqCfgFilePath)
	if err != nil {
		return nil, fmt.Errorf("获取ReqCfg文件信息失败: %w", err)
	}

	tool := &DataBaseTool{
		dbs:            dbs,
		dbConfigs:      dbConfigs,
		requests:       requests,
		requestsByName: requestsByName,
		aesKey:         aesKey,
		dbCfgFilePath:  dbCfgFilePath,
		reqCfgFilePath: reqCfgFilePath,
		dbCfgModTime:   dbCfgFileInfo.ModTime(),
		reqCfgModTime:  reqCfgFileInfo.ModTime(),
		stopMonitor:    make(chan struct{}),
	}

	// 启动配置文件监控
	go tool.monitorConfigFile()

	return tool, nil
}

// CallProcedure 调用存储过程的通用函数
// requestID: 请求ID（对应XML中的Request ID）
// inputs: 输入参数（按照InputParam定义的顺序传入）
// 返回: (业务输出参数map, Out_ReturnMsg, Out_Result, error)
func (dt *DataBaseTool) CallProcedure(requestID int, inputs ...interface{}) (map[string]interface{}, string, int, error) {
	dt.mu.RLock()
	reqDef, exists := dt.requests[requestID]
	dt.mu.RUnlock()

	if !exists {
		return nil, "", 0, fmt.Errorf("未找到请求ID: %d", requestID)
	}

	return dt.executeProcedure(reqDef, inputs...)
}

// CallProcedureByName 通过存储过程名称调用存储过程
// procedureName: 存储过程名称（对应XML中的StoredProcedure属性）
// inputs: 输入参数（按照InputParam定义的顺序传入）
// 返回: (业务输出参数map, Out_ReturnMsg, Out_Result, error)
func (dt *DataBaseTool) CallProcedureByName(procedureName string, inputs ...interface{}) (map[string]interface{}, string, int, error) {
	dt.mu.RLock()
	reqDef, exists := dt.requestsByName[procedureName]
	dt.mu.RUnlock()

	if !exists {
		return nil, "", 0, fmt.Errorf("未找到存储过程: %s", procedureName)
	}

	return dt.executeProcedure(reqDef, inputs...)
}

// executeProcedure 执行存储过程的内部实现
func (dt *DataBaseTool) executeProcedure(reqDef *RequestDef, inputs ...interface{}) (map[string]interface{}, string, int, error) {
	// 1. 根据 ClusterID 获取对应的数据库连接
	db, exists := dt.dbs[reqDef.ClusterID]
	if !exists {
		return nil, "", 0, fmt.Errorf("未找到 ClusterID=%d 的数据库连接", reqDef.ClusterID)
	}

	// 2. 验证输入参数数量
	if len(inputs) != len(reqDef.InputParams) {
		return nil, "", 0, fmt.Errorf("输入参数数量不匹配: 期望 %d 个，实际 %d 个",
			len(reqDef.InputParams), len(inputs))
	}

	// 3. 验证输入参数类型
	for i, param := range reqDef.InputParams {
		if err := validateParamType(inputs[i], param.Type); err != nil {
			return nil, "", 0, fmt.Errorf("参数 %s 类型验证失败: %w", param.Name, err)
		}
	}

	// 4. 准备输出参数（XML定义的参数 + 自动添加的Out_ReturnMsg和Out_Result）
	outputValues := make([]interface{}, len(reqDef.OutputParams)+2)
	for i, param := range reqDef.OutputParams {
		outputValues[i] = createOutputVariable(param.Type)
	}
	// 自动添加的两个标准输出参数
	var outReturnMsg string
	var outResult int
	outputValues[len(reqDef.OutputParams)] = &outReturnMsg
	outputValues[len(reqDef.OutputParams)+1] = &outResult

	// 5. 构建SQL调用语句
	sqlQuery := dt.buildProcedureCallSQL(reqDef, inputs)

	// 6. 执行存储过程
	// 使用 simple protocol，CALL 语句可以直接返回 OUT 参数
	rows, err := db.Session(&gorm.Session{
		PrepareStmt: false,
	}).Raw(sqlQuery).Rows()
	if err != nil {
		return nil, "", 0, fmt.Errorf("调用存储过程失败: %w", err)
	}
	defer rows.Close()

	// 检查是否有结果
	if !rows.Next() {
		return nil, "", 0, fmt.Errorf("存储过程未返回结果")
	}

	// 扫描结果
	err = rows.Scan(outputValues...)
	if err != nil {
		return nil, "", 0, fmt.Errorf("扫描存储过程结果失败: %w", err)
	}

	// 7. 处理输出参数
	result := make(map[string]interface{})
	for i, param := range reqDef.OutputParams {
		value := outputValues[i]

		// 如果是jsonb类型，反序列化JSON
		if param.Type == "jsonb" {
			// 处理 NullString 类型
			if nullStr, ok := value.(*sql.NullString); ok {
				if nullStr.Valid && nullStr.String != "" {
					jsonValue, err := dt.deserializeJSON(nullStr.String, reqDef.OutputRecord)
					if err != nil {
						return nil, "", 0, fmt.Errorf("反序列化JSON失败 (%s): %w", param.Name, err)
					}
					result[param.Name] = jsonValue
				} else {
					result[param.Name] = nil
				}
			} else {
				result[param.Name] = nil
			}
		} else {
			// 其他类型直接返回
			result[param.Name] = dereferenceValue(value)
		}
	}

	// 返回业务输出参数和标准输出参数（分离返回）
	return result, outReturnMsg, outResult, nil
}

// executeProcedureWithPreparedStmt 在 prepared statement 模式下执行存储过程
// 使用临时表来存储和获取输出参数
func (dt *DataBaseTool) executeProcedureWithPreparedStmt(db *gorm.DB, reqDef *RequestDef, inputs []interface{}, outputValues []interface{}) error {
	// 获取 schema
	schema := "public"
	if dbConfig, exists := dt.dbConfigs[reqDef.ClusterID]; exists {
		if dbConfig.Schema != "" {
			schema = dbConfig.Schema
		}
	}

	// 开始事务
	tx := db.Session(&gorm.Session{PrepareStmt: true}).Begin()
	if tx.Error != nil {
		return fmt.Errorf("开始事务失败: %w", tx.Error)
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// 1. 创建临时表来存储输出参数
	tempTableName := fmt.Sprintf("temp_proc_output_%d", time.Now().UnixNano())

	// 构建临时表字段定义
	var fieldDefs []string
	for i, param := range reqDef.OutputParams {
		fieldDefs = append(fieldDefs, fmt.Sprintf("out_%d %s", i, param.Type))
	}
	fieldDefs = append(fieldDefs, "out_return_msg varchar")
	fieldDefs = append(fieldDefs, "out_result int")

	createTableSQL := fmt.Sprintf("CREATE TEMP TABLE %s (%s)", tempTableName, strings.Join(fieldDefs, ", "))
	if err := tx.Exec(createTableSQL).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("创建临时表失败: %w", err)
	}

	// 2. 构建 DO 块来执行存储过程并插入结果到临时表
	var paramNames []string
	var paramDecls []string
	var insertValues []string

	// 输出参数声明
	for i, param := range reqDef.OutputParams {
		varName := fmt.Sprintf("v_out_%d", i)
		paramNames = append(paramNames, varName)
		paramDecls = append(paramDecls, fmt.Sprintf("%s %s;", varName, param.Type))
		insertValues = append(insertValues, varName)
	}

	// 标准输出参数
	paramNames = append(paramNames, "v_out_return_msg", "v_out_result")
	paramDecls = append(paramDecls, "v_out_return_msg varchar;", "v_out_result int;")
	insertValues = append(insertValues, "v_out_return_msg", "v_out_result")

	// 构建输入参数
	var inputParams []string
	for i, input := range inputs {
		formattedVal := dt.formatSQLValue(input)
		inputParams = append(inputParams, dt.castToType(formattedVal, reqDef.InputParams[i].Type))
	}

	// 构建 CALL 语句的参数列表（输入参数 + 输出参数变量）
	var callParams []string
	callParams = append(callParams, inputParams...)
	for _, varName := range paramNames {
		callParams = append(callParams, varName)
	}

	doBlockSQL := fmt.Sprintf(`
DO $$
DECLARE
    %s
BEGIN
    CALL "%s"."%s"(%s);
    INSERT INTO %s VALUES (%s);
END $$;
`, strings.Join(paramDecls, "\n    "), schema, reqDef.StoredProcedure, strings.Join(callParams, ", "), tempTableName, strings.Join(insertValues, ", "))

	if err := tx.Exec(doBlockSQL).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("执行存储过程失败: %w", err)
	}

	// 3. 从临时表读取结果
	selectSQL := fmt.Sprintf("SELECT * FROM %s", tempTableName)
	rows, err := tx.Raw(selectSQL).Rows()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("读取结果失败: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		tx.Rollback()
		return fmt.Errorf("存储过程未返回结果")
	}

	if err := rows.Scan(outputValues...); err != nil {
		tx.Rollback()
		return fmt.Errorf("扫描结果失败: %w", err)
	}

	// 4. 提交事务
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	return nil
}

// StopMonitoring 停止文件监控
func (dt *DataBaseTool) StopMonitoring() {
	close(dt.stopMonitor)
}

// monitorConfigFile 监控配置文件变化
func (dt *DataBaseTool) monitorConfigFile() {
	ticker := time.NewTicker(5 * time.Second) // 每5秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 检查 DbCfg 文件变化
			dbCfgFileInfo, err := os.Stat(dt.dbCfgFilePath)
			if err == nil && dbCfgFileInfo.ModTime().After(dt.dbCfgModTime) {
				// DbCfg 文件已修改，重新加载数据库连接
				if err := dt.reloadDbConfig(); err != nil {
					// 重新加载失败，保持原有配置
					fmt.Printf("重新加载DbCfg失败: %v\n", err)
				} else {
					dt.dbCfgModTime = dbCfgFileInfo.ModTime()
					fmt.Println("DbCfg配置已重新加载")
				}
			}

			// 检查 ReqCfg 文件变化
			reqCfgFileInfo, err := os.Stat(dt.reqCfgFilePath)
			if err == nil && reqCfgFileInfo.ModTime().After(dt.reqCfgModTime) {
				// ReqCfg 文件已修改，重新加载请求配置
				if err := dt.reloadReqConfig(); err != nil {
					// 重新加载失败，保持原有配置
					fmt.Printf("重新加载ReqCfg失败: %v\n", err)
				} else {
					dt.reqCfgModTime = reqCfgFileInfo.ModTime()
					fmt.Println("ReqCfg配置已重新加载")
				}
			}

		case <-dt.stopMonitor:
			return
		}
	}
}

// reloadDbConfig 重新加载数据库配置
func (dt *DataBaseTool) reloadDbConfig() error {
	// 1. 解密数据库配置文件
	dbCfgContent, err := decryptConfigFile(dt.aesKey, dt.dbCfgFilePath)
	if err != nil {
		return fmt.Errorf("解密数据库配置文件失败: %w", err)
	}

	// 2. 解析数据库配置XML
	var dbCfg DbCfgSystem
	err = xml.Unmarshal([]byte(dbCfgContent), &dbCfg)
	if err != nil {
		return fmt.Errorf("解析数据库配置XML失败: %w", err)
	}

	// 3. 关闭旧的数据库连接
	dt.mu.Lock()
	for _, db := range dt.dbs {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	dt.mu.Unlock()

	// 4. 建立新的数据库连接
	dbs := make(map[int]*gorm.DB)
	dbConfigs := make(map[int]*DatabaseConfig)

	for i := range dbCfg.Clusters {
		cluster := &dbCfg.Clusters[i]
		dbConfig := &cluster.Database

		// 解析 DataSource (格式: IP:Port)
		host := dbConfig.DataSource
		port := "5432"
		if idx := strings.Index(dbConfig.DataSource, ":"); idx != -1 {
			host = dbConfig.DataSource[:idx]
			port = dbConfig.DataSource[idx+1:]
		}

		// 构建 PostgreSQL DSN
		dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			host,
			port,
			dbConfig.UserID,
			dbConfig.Password,
			dbConfig.InitialCatalog,
		)

		// 配置 PostgreSQL 驱动
		pgConfig := postgres.Config{
			DSN:                  dsn,
			PreferSimpleProtocol: true,
		}
		dialector := postgres.New(pgConfig)

		// 连接数据库
		db, err := gorm.Open(dialector, &gorm.Config{
			PrepareStmt:            false,
			SkipDefaultTransaction: true,
		})
		if err != nil {
			return fmt.Errorf("连接数据库失败 (ClusterID=%d): %w", dbConfig.ID, err)
		}

		// 配置连接池
		sqlDB, err := db.DB()
		if err != nil {
			return fmt.Errorf("获取底层数据库连接失败 (ClusterID=%d): %w", dbConfig.ID, err)
		}

		maxOpenConns := cluster.ConnectionPool.Max
		if maxOpenConns <= 0 {
			maxOpenConns = 10
		}
		sqlDB.SetMaxOpenConns(maxOpenConns)
		sqlDB.SetMaxIdleConns(maxOpenConns / 2)
		sqlDB.SetConnMaxLifetime(time.Hour)

		dbs[dbConfig.ID] = db
		dbConfigs[dbConfig.ID] = dbConfig
	}

	// 5. 更新配置（使用写锁）
	dt.mu.Lock()
	dt.dbs = dbs
	dt.dbConfigs = dbConfigs
	dt.mu.Unlock()

	return nil
}

// reloadReqConfig 重新加载请求配置文件
func (dt *DataBaseTool) reloadReqConfig() error {
	// 1. 解密请求配置文件
	reqCfgContent, err := decryptConfigFile(dt.aesKey, dt.reqCfgFilePath)
	if err != nil {
		return fmt.Errorf("解密请求配置文件失败: %w", err)
	}

	// 2. 解析请求配置XML
	var reqCfg DbSystemConfig
	err = xml.Unmarshal([]byte(reqCfgContent), &reqCfg)
	if err != nil {
		return fmt.Errorf("解析请求配置XML失败: %w", err)
	}

	// 3. 构建新的请求映射
	requests := make(map[int]*RequestDef)
	requestsByName := make(map[string]*RequestDef)
	for i := range reqCfg.Requests {
		req := &reqCfg.Requests[i]
		requests[req.ID] = req
		requestsByName[req.StoredProcedure] = req
	}

	// 4. 更新配置（使用写锁）
	dt.mu.Lock()
	dt.requests = requests
	dt.requestsByName = requestsByName
	dt.mu.Unlock()

	return nil
}

// GetRequestByID 获取请求定义（通过ID）
func (dt *DataBaseTool) GetRequestByID(requestID int) (*RequestDef, bool) {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	req, exists := dt.requests[requestID]
	return req, exists
}

// GetRequestByName 获取请求定义（通过存储过程名称）
func (dt *DataBaseTool) GetRequestByName(procedureName string) (*RequestDef, bool) {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	req, exists := dt.requestsByName[procedureName]
	return req, exists
}

// ListAllRequests 列出所有请求定义
func (dt *DataBaseTool) ListAllRequests() []*RequestDef {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	requests := make([]*RequestDef, 0, len(dt.requests))
	for _, req := range dt.requests {
		requests = append(requests, req)
	}
	return requests
}

// buildProcedureCallSQL 构建存储过程调用SQL
func (dt *DataBaseTool) buildProcedureCallSQL(reqDef *RequestDef, inputs []interface{}) string {
	// 构建参数列表
	var params []string

	// 添加输入参数（带类型转换）
	for i, input := range inputs {
		formattedVal := dt.formatSQLValue(input)
		// 为输入参数也添加类型转换，确保类型匹配
		params = append(params, dt.castToType(formattedVal, reqDef.InputParams[i].Type))
	}

	// 添加输出参数占位符（使用默认值并指定类型）
	for _, param := range reqDef.OutputParams {
		defaultVal := dt.formatSQLValue(getDefaultValue(param.Type))
		// 为输出参数添加类型转换
		params = append(params, dt.castToType(defaultVal, param.Type))
	}

	// 自动添加标准输出参数 Out_ReturnMsg 和 Out_Result
	params = append(params, "''::varchar") // Out_ReturnMsg
	params = append(params, "0::int")      // Out_Result

	// 根据 ClusterID 获取 Schema 信息
	schema := "public" // 默认 schema
	if dbConfig, exists := dt.dbConfigs[reqDef.ClusterID]; exists {
		if dbConfig.Schema != "" {
			schema = dbConfig.Schema
		}
	}

	// 构建完整的CALL语句（使用配置中的 schema）
	sqlQuery := fmt.Sprintf(`CALL "%s"."%s"(%s)`, schema, reqDef.StoredProcedure, strings.Join(params, ", "))
	return sqlQuery
}

// castToType 为值添加PostgreSQL类型转换
func (dt *DataBaseTool) castToType(value string, paramType string) string {
	switch paramType {
	case "jsonb":
		return fmt.Sprintf("%s::jsonb", value)
	case "bytea":
		return fmt.Sprintf("%s::bytea", value)
	case "bigint":
		return fmt.Sprintf("%s::bigint", value)
	case "int":
		return fmt.Sprintf("%s::int", value)
	case "smallint":
		return fmt.Sprintf("%s::smallint", value)
	case "varchar":
		return fmt.Sprintf("%s::varchar", value)
	case "text":
		return fmt.Sprintf("%s::text", value)
	default:
		return value
	}
}

// formatSQLValue 格式化SQL值
func (dt *DataBaseTool) formatSQLValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case string:
		// 转义单引号
		escaped := strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case []byte:
		// bytea类型使用十六进制格式
		return fmt.Sprintf("'\\x%x'", v)
	default:
		// 其他类型转为字符串
		return fmt.Sprintf("'%v'", v)
	}
}

// validateParamType 验证参数类型
func validateParamType(value interface{}, expectedType string) error {
	if value == nil {
		return nil // 允许nil值
	}

	valueType := reflect.TypeOf(value)
	actualKind := valueType.Kind()

	switch expectedType {
	case "bigint":
		if actualKind != reflect.Int64 && actualKind != reflect.Int {
			return fmt.Errorf("期望 int64，实际 %v", valueType)
		}
	case "int":
		if actualKind != reflect.Int && actualKind != reflect.Int32 && actualKind != reflect.Int64 {
			return fmt.Errorf("期望 int，实际 %v", valueType)
		}
	case "smallint":
		if actualKind != reflect.Int16 && actualKind != reflect.Int && actualKind != reflect.Int32 {
			return fmt.Errorf("期望 int16，实际 %v", valueType)
		}
	case "varchar", "text":
		if actualKind != reflect.String {
			return fmt.Errorf("期望 string，实际 %v", valueType)
		}
	case "bytea":
		if actualKind != reflect.Slice || valueType.Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("期望 []byte，实际 %v", valueType)
		}
	case "jsonb":
		// jsonb可以接受string或[]byte
		if actualKind != reflect.String && (actualKind != reflect.Slice || valueType.Elem().Kind() != reflect.Uint8) {
			return fmt.Errorf("期望 string 或 []byte，实际 %v", valueType)
		}
	}

	return nil
}

// createOutputVariable 创建输出变量（使用可空类型）
func createOutputVariable(paramType string) interface{} {
	switch paramType {
	case "bigint":
		return &sql.NullInt64{}
	case "int":
		return &sql.NullInt32{}
	case "smallint":
		return &sql.NullInt16{}
	case "varchar", "text", "jsonb":
		return &sql.NullString{}
	case "bytea":
		var v []byte
		return &v
	default:
		return &sql.NullString{}
	}
}

// getDefaultValue 获取类型的默认值
func getDefaultValue(paramType string) interface{} {
	switch paramType {
	case "bigint", "int", "smallint":
		return 0
	case "varchar", "text":
		return ""
	case "jsonb":
		return "{}"
	case "bytea":
		return []byte{}
	default:
		return ""
	}
}

// dereferenceValue 解引用指针值并处理可空类型
func dereferenceValue(value interface{}) interface{} {
	// 处理 sql.Null* 类型
	switch v := value.(type) {
	case *sql.NullInt64:
		if v.Valid {
			return v.Int64
		}
		return nil
	case *sql.NullInt32:
		if v.Valid {
			return v.Int32
		}
		return nil
	case *sql.NullInt16:
		if v.Valid {
			return v.Int16
		}
		return nil
	case *sql.NullString:
		if v.Valid {
			return v.String
		}
		return nil
	}

	// 处理普通指针
	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		return val.Elem().Interface()
	}
	return value
}

// deserializeJSON 反序列化JSON
func (dt *DataBaseTool) deserializeJSON(jsonStr string, outputRecord *OutputRecord) (interface{}, error) {
	if jsonStr == "" || jsonStr == "{}" || jsonStr == "[]" {
		return nil, nil
	}

	// 尝试解析为数组或对象
	var result interface{}
	err := json.Unmarshal([]byte(jsonStr), &result)
	if err != nil {
		return nil, err
	}

	// 如果有OutputRecord定义，可以进一步处理为强类型
	// 这里返回通用的map或slice
	return result, nil
}

// decryptConfigFile 解密配置文件（从correct_aes_decrypt.go复制）
func decryptConfigFile(aesKey string, encryptedFilePath string) (string, error) {
	dataHex, err := ioutil.ReadFile(encryptedFilePath)
	if err != nil {
		return "", fmt.Errorf("读取文件失败: %w", err)
	}

	bData, err := hex.DecodeString(strings.TrimSpace(string(dataHex)))
	if err != nil {
		return "", fmt.Errorf("十六进制解码失败: %w", err)
	}

	result, err := aesDecryptECB(bData, []byte(aesKey))
	if err != nil {
		return "", fmt.Errorf("AES解密失败: %w", err)
	}

	return string(result), nil
}

// aesDecryptECB AES-ECB解密
func aesDecryptECB(encrypted []byte, key []byte) (decrypted []byte, err error) {
	cipher, err := aes.NewCipher(generateKey(key))
	if err != nil {
		return nil, err
	}
	decrypted = make([]byte, len(encrypted))

	for bs, be := 0, cipher.BlockSize(); bs < len(encrypted); bs, be = bs+cipher.BlockSize(), be+cipher.BlockSize() {
		cipher.Decrypt(decrypted[bs:be], encrypted[bs:be])
	}

	trim := 0
	if len(decrypted) > 0 {
		trim = len(decrypted) - int(decrypted[len(decrypted)-1])
	}

	return decrypted[:trim], nil
}

// generateKey 生成密钥
func generateKey(key []byte) (genKey []byte) {
	genKey = make([]byte, 16)
	copy(genKey, key)
	for i := 16; i < len(key); {
		for j := 0; j < 16 && i < len(key); j, i = j+1, i+1 {
			genKey[j] ^= key[i]
		}
	}
	return genKey
}

// ============================================================================
// 事务支持
// ============================================================================

// Transaction 事务对象，用于在一个事务中执行多个存储过程调用
type Transaction struct {
	tx      *gorm.DB
	dt      *DataBaseTool
	db      *gorm.DB
	results []TransactionResult
}

// TransactionResult 事务中单个存储过程调用的结果
type TransactionResult struct {
	RequestID  int                    // 请求ID
	Result     map[string]interface{} // 业务输出参数
	ReturnMsg  string                 // 返回消息
	ReturnCode int                    // 返回码
	Error      error                  // 错误信息
}

// BeginTransaction 开始一个事务
// clusterID: 数据库集群ID（事务中的所有操作必须在同一个数据库上）
func (dt *DataBaseTool) BeginTransaction(clusterID int) (*Transaction, error) {
	dt.mu.RLock()
	db, exists := dt.dbs[clusterID]
	dt.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("未找到 ClusterID=%d 的数据库连接", clusterID)
	}

	// 开始事务
	tx := db.Session(&gorm.Session{
		PrepareStmt: false,
	}).Begin()

	if tx.Error != nil {
		return nil, fmt.Errorf("开始事务失败: %w", tx.Error)
	}

	return &Transaction{
		tx:      tx,
		dt:      dt,
		db:      db,
		results: make([]TransactionResult, 0),
	}, nil
}

// CallProcedure 在事务中调用存储过程
// requestID: 请求ID
// inputs: 输入参数
func (t *Transaction) CallProcedure(requestID int, inputs ...interface{}) error {
	// 获取请求定义
	t.dt.mu.RLock()
	reqDef, exists := t.dt.requests[requestID]
	t.dt.mu.RUnlock()

	if !exists {
		err := fmt.Errorf("未找到 Request ID=%d 的定义", requestID)
		t.results = append(t.results, TransactionResult{
			RequestID: requestID,
			Error:     err,
		})
		return err
	}

	// 验证输入参数数量
	if len(inputs) != len(reqDef.InputParams) {
		err := fmt.Errorf("输入参数数量不匹配: 期望 %d 个，实际 %d 个",
			len(reqDef.InputParams), len(inputs))
		t.results = append(t.results, TransactionResult{
			RequestID: requestID,
			Error:     err,
		})
		return err
	}

	// 验证输入参数类型
	for i, param := range reqDef.InputParams {
		if err := validateParamType(inputs[i], param.Type); err != nil {
			err := fmt.Errorf("参数 %s 类型验证失败: %w", param.Name, err)
			t.results = append(t.results, TransactionResult{
				RequestID: requestID,
				Error:     err,
			})
			return err
		}
	}

	// 准备输出参数
	outputValues := make([]interface{}, len(reqDef.OutputParams)+2)
	for i, param := range reqDef.OutputParams {
		outputValues[i] = createOutputVariable(param.Type)
	}
	var outReturnMsg string
	var outResult int
	outputValues[len(reqDef.OutputParams)] = &outReturnMsg
	outputValues[len(reqDef.OutputParams)+1] = &outResult

	// 构建SQL调用语句
	sqlQuery := t.dt.buildProcedureCallSQL(reqDef, inputs)

	// 执行存储过程
	rows, err := t.tx.Raw(sqlQuery).Rows()
	if err != nil {
		err := fmt.Errorf("调用存储过程失败: %w", err)
		t.results = append(t.results, TransactionResult{
			RequestID: requestID,
			Error:     err,
		})
		return err
	}
	defer rows.Close()

	// 检查是否有结果
	if !rows.Next() {
		err := fmt.Errorf("存储过程未返回结果")
		t.results = append(t.results, TransactionResult{
			RequestID: requestID,
			Error:     err,
		})
		return err
	}

	// 扫描结果
	err = rows.Scan(outputValues...)
	if err != nil {
		err := fmt.Errorf("扫描存储过程结果失败: %w", err)
		t.results = append(t.results, TransactionResult{
			RequestID: requestID,
			Error:     err,
		})
		return err
	}

	// 处理输出参数
	result := make(map[string]interface{})
	for i, param := range reqDef.OutputParams {
		value := outputValues[i]

		// 如果是jsonb类型，反序列化JSON
		if param.Type == "jsonb" {
			if nullStr, ok := value.(*sql.NullString); ok {
				if nullStr.Valid && nullStr.String != "" {
					jsonValue, err := t.dt.deserializeJSON(nullStr.String, reqDef.OutputRecord)
					if err != nil {
						err := fmt.Errorf("反序列化JSON失败 (%s): %w", param.Name, err)
						t.results = append(t.results, TransactionResult{
							RequestID: requestID,
							Error:     err,
						})
						return err
					}
					result[param.Name] = jsonValue
				} else {
					result[param.Name] = nil
				}
			}
		} else {
			result[param.Name] = dereferenceValue(value)
		}
	}

	// 保存结果
	t.results = append(t.results, TransactionResult{
		RequestID:  requestID,
		Result:     result,
		ReturnMsg:  outReturnMsg,
		ReturnCode: outResult,
		Error:      nil,
	})

	return nil
}

// CallProcedureByName 在事务中通过名称调用存储过程
func (t *Transaction) CallProcedureByName(procedureName string, inputs ...interface{}) error {
	t.dt.mu.RLock()
	reqDef, exists := t.dt.requestsByName[procedureName]
	t.dt.mu.RUnlock()

	if !exists {
		err := fmt.Errorf("未找到存储过程: %s", procedureName)
		t.results = append(t.results, TransactionResult{
			Error: err,
		})
		return err
	}

	return t.CallProcedure(reqDef.ID, inputs...)
}

// Commit 提交事务并返回所有结果
func (t *Transaction) Commit() ([]TransactionResult, error) {
	if err := t.tx.Commit().Error; err != nil {
		return t.results, fmt.Errorf("提交事务失败: %w", err)
	}
	return t.results, nil
}

// Rollback 回滚事务
func (t *Transaction) Rollback() error {
	if err := t.tx.Rollback().Error; err != nil {
		return fmt.Errorf("回滚事务失败: %w", err)
	}
	return nil
}

// GetResults 获取当前所有调用结果（不提交事务）
func (t *Transaction) GetResults() []TransactionResult {
	return t.results
}
