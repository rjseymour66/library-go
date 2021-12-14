package config

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
