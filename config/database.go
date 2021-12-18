package config

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/rjseymour66/library-go/values"
)

var (
	GetDatabaseConnectionString      = getDatabaseConnectionString
	GetDatabaseMaxIdleConnections    = getDatabaseMaxIdleConnections
	GetDatabaseMaxOpenConnections    = getDatabaseMaxOpenConnections
	GetDatabaseConnectionMaxLifetime = getDatabaseConnectionMaxLifetime
)

func getDatabaseConnectionString() string {
	return getConfigString("database.connection_string")
}

func getDatabaseMaxIdleConnections() int {
	return getConfigInt("database.max_idle_connections")
}

func getDatabaseMaxOpenConnections() int {
	return getConfigInt("database.max_open_connections")
}

func getDatabaseConnectionMaxLifetime() time.Duration {
	return getConfigDuration("database.connection_max_lifetime")
}

var (
	InitializeDb    = initializeDb
	PrepareDbRunner = prepareDbRunner
)

func initializeDb() (err error) {
	connectionString := GetDatabaseConnectionString()
	maxIdleConnections := GetDatabaseMaxIdleConnections()
	maxOpenConnections := GetDatabaseMaxOpenConnections()
	connectionMaxLifetime := GetDatabaseConnectionMaxLifetime()

	dbHandler, err = initDbHandle("master", "postgres",
		connectionString,
		maxIdleConnections,
		maxOpenConnections,
		connectionMaxLifetime,
	)

	if err != nil {
		return
	}

	return
}

var dbHandler *sql.DB

func initDbHandle(
	name, dbType, connectionString string,
	maxIdleConnections, maxOpenConnections int,
	connectionMaxLifetime time.Duration,
) (dbHandler *sql.DB, err error) {

	if dbType == "" {
		return nil, errors.New("Database type is empty")
	}

	if connectionString == "" {
		return nil, errors.New("Connection string is empty")
	}

	dbHandler, err = sql.Open(dbType, connectionString)
	if err != nil {
		return nil, err
	}

	// initialize the connection pool
	dbHandler.SetMaxIdleConns(maxIdleConnections)
	dbHandler.SetMaxOpenConns(maxOpenConnections)
	dbHandler.SetConnMaxLifetime(connectionMaxLifetime)

	err = validateDB(dbHandler)

	if err != nil {
		dbHandler.Close()
	}

	return
}

func validateDB(dbHandler *sql.DB) (err error) {
	err = dbHandler.Ping()
	if err != nil {
		return
	}

	timeZone, err := readDatabaseTimeZone(context.Background(), dbHandler)

	if err != nil {
		return
	}

	if timeZone != "UTC" {
		err = fmt.Errorf("Database 'timezone' must be set to 'UTC'. Currently, it is '%v'", timeZone)
		return
	}

	return
}

func readDatabaseTimeZone(ctx context.Context, dbHandler *sql.DB) (timeZone string, err error) {
	rowsTimeZone, err := dbHandler.QueryContext(ctx, "show timezone")

	if err != nil {
		return
	}

	defer rowsTimeZone.Close()

	if !rowsTimeZone.Next() {
		err = fmt.Errorf("No time zone")
		return
	}

	err = rowsTimeZone.Scan(&timeZone)

	return
}

func createRunner(db *sql.DB) Runner {
	run := new(dbRunner)
	run.db = db
	return run
}

func prepareDbRunner(ctx context.Context) context.Context {
	return context.WithValue(ctx, values.ContextKeyDbRunner, createRunner(dbHandler))
}

type dbRunner struct {
	db      *sql.DB
	tx      *sql.Tx
	conn    *sql.Conn
	txCount int
}

// Runner is an interface for db access
type Runner interface {
	Transact(ctx context.Context, txOptions *sql.TxOptions, txFunc func() error) error
	Conn(ctx context.Context, connFunc func() error) error
	Query(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error)
	QueryRow(ctx context.Context, query string, args ...interface{}) (row *sql.Row)
	Exec(ctx context.Context, query string, args ...interface{}) (res sql.Result, err error)
	Prepare(ctx context.Context, query string) (stmt *sql.Stmt, err error)
	IsInTranscation() bool
}

func (run *dbRunner) Transact(ctx context.Context, txOptions *sql.TxOptions, txFunc func() error) (err error) {

	if run.tx == nil {
		var tx *sql.Tx

		if run.conn == nil {
			tx, err = run.db.BeginTx(ctx, txOptions)
		} else {
			tx, err = run.conn.BeginTx(ctx, txOptions)
		}

		if err != nil {
			return err
		}

		run.tx = tx
		run.txCount = 1
	} else {
		run.txCount++
	}

	defer func() {

		// Recover from panic
		p := recover()

		// Rollback the transaction in case of error or panic
		if (err != nil || p != nil) && run.tx != nil {
			run.tx.Rollback() // ignoring the error
			run.tx = nil
			run.txCount = 0
		}

		// Re-panic if it was panicking
		if p != nil {
			panic(p)
		}

		if err != nil {
			return
		}

		if run.tx == nil {
			panic("Transaction is already rolledback or committed")
		}

		// Decrement tx counter and commit tx if its the
		// last one in the chain
		run.txCount--
		if run.txCount == 0 {
			err = run.tx.Commit()
			if err == sql.ErrTxDone {
				ctxErr := ctx.Err()
				if ctxErr == context.Canceled || ctxErr == context.DeadlineExceeded {
					err = ctxErr
				}
			}

			run.tx = nil
		}

	}()

	err = txFunc()

	return
}

func (run *dbRunner) Conn(ctx context.Context, connFunc func() error) (err error) {
	// If it is in transaction or already using single connection
	// just call the function
	if run.tx != nil || run.conn != nil {
		return connFunc()
	}

	run.conn, err = run.db.Conn(ctx)

	if err == driver.ErrBadConn {
		run.conn, err = run.db.Conn(ctx)
	}

	if err != nil {
		return
	}

	defer func() {
		errClose := run.conn.Close()
		run.conn = nil
		if err != nil {
			err = errClose
		}
	}()

	err = connFunc()

	return
}

func (run *dbRunner) Query(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error) {
	if run.tx != nil {
		rows, err = run.tx.QueryContext(ctx, query, args...)
	} else if run.conn != nil {
		rows, err = run.conn.QueryContext(ctx, query, args...)
	} else {
		rows, err = run.db.QueryContext(ctx, query, args...)
	}

	return
}

func (run *dbRunner) QueryRow(ctx context.Context, query string, args ...interface{}) (row *sql.Row) {

	if run.tx != nil {
		row = run.tx.QueryRowContext(ctx, query, args...)
	} else if run.conn != nil {
		row = run.conn.QueryRowContext(ctx, query, args...)
	} else {
		row = run.db.QueryRowContext(ctx, query, args...)
	}

	return
}

func (run *dbRunner) Exec(ctx context.Context, query string, args ...interface{}) (res sql.Result, err error) {

	if run.tx != nil {
		res, err = run.tx.ExecContext(ctx, query, args...)
	} else if run.conn != nil {
		res, err = run.conn.ExecContext(ctx, query, args...)
	} else {
		res, err = run.db.ExecContext(ctx, query, args...)
	}

	return
}

func (run *dbRunner) Prepare(ctx context.Context, query string) (stmt *sql.Stmt, err error) {

	if run.tx != nil {
		stmt, err = run.tx.PrepareContext(ctx, query)
	} else if run.conn != nil {
		stmt, err = run.conn.PrepareContext(ctx, query)
	} else {
		stmt, err = run.db.PrepareContext(ctx, query)
	}

	return
}

func (run *dbRunner) IsInTransaction() bool {
	return run.txCount > 0
}

type rowReader struct {
	rows      *sql.Rows
	columns   []string
	values    []interface{}
	valuePtrs []interface{}
	lastError error
}

// Simplies reading sql.Rows objects
type RowReader interface {
	ScanNext() bool
	Error() error
	RowReaderFxs
}

func (rr *rowReader) ScanNext(hasMore bool) {
	if hasMore = rr.rows.Next(); hasMore {
		err := rr.rows.Scan(rr.valuePtrs...)
		rr.lastError = err
		if err != nil {
			hasMore = false
		}
	}
	return
}

func (rr *rowReader) Error() error {
	return rr.lastError
}

// Methods that read values from a single row in sql.Rows
type RowReaderFxs interface {
	ReadByIdxString(columnIdx int) string
	ReadByIdxInt64(columnIdx int) int64
	ReadByIdxTime(columnIdx int) time.Time
	ReadAllToStruct(p interface{})
}

// Errors
var (
	ErrorNullValue   = errors.New("Null value encountered")
	ErrorWrongType   = errors.New("Unable to convert type")
	ErrorUnsupported = errors.New("Unsupported type")
)

func (rr *rowReader) ReadByIdxString(columnIdx int) string {
	switch value := rr.values[columnIdx].(type) {
	case string:
		return value
	case []byte:
		return string(value)
	case nil:
		panic(ErrorNullValue)
	default:
		panic(ErrorWrongType)
	}
}

func (rr *rowReader) ReadByIdxInt64(columnIdx int) int64 {
	switch value := rr.values[columnIdx].(type) {
	case int64:
		return value
	case []byte:
		s := string(value)
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			panic(ErrorWrongType)
		}
		return i
	case nil:
		panic(ErrorNullValue)
	default:
		panic(ErrorWrongType)
	}
}

func (rr *rowReader) ReadByIdxTime(columnIdx int) time.Time {
	switch value := rr.values[columnIdx].(type) {
	case time.Time:
		return value
	case []byte:
		time, err := time.Parse(time.RFC3339Nano, string(value))
		if err != nil {
			panic(ErrorWrongType)
		}
		return time
	case string:
		time, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			panic(ErrorWrongType)
		}
		return time
	case nil:
		panic(ErrorNullValue)
	default:
		panic(ErrorWrongType)
	}
}

func (rr *rowReader) ReadAllToStruct(p interface{}) {
	var value reflect.Value
	value = reflect.ValueOf(p)
	if value.Kind() != reflect.Ptr {
		return
	}

	value = reflect.Indirect(value)
	if value.Kind() != reflect.Struct {
		return
	}

	for columnIdx, columnName := range rr.columns {
		if rr.values[columnIdx] == nil {
			continue
		}

		column := value.FieldByName(columnName)
		if column == (reflect.Value{}) {
			continue
		}

		switch column.Kind() {
		case reflect.String:
			column.SetString(rr.ReadByIdxString(columnIdx))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			column.SetInt(rr.ReadByIdxInt64(columnIdx))
		default:
			panic(ErrorUnsupported)
		}
	}
}

var (
	// Returns RowReader interface to read data from sql.Rows
	GetRowReader = getRowReader
)

func getRowReader(rows *sql.Rows) (RowReader, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	n := len(columns)

	rr := new(rowReader)
	rr.rows = rows
	rr.columns = columns
	rr.values = make([]interface{}, n)
	for i := 0; i < n; i++ {
		rr.valuePtrs[i] = &rr.values[i]
	}
	return rr, nil
}
