package messagebusarhc

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
)

var createTableStatements = []string{
	`CREATE DATABASES IF NOT EXISTS eventsarray EFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE 'utf8_general_ci';`,
	`USE eventsarray;`,
	`CREATE TABLE IF NOT EXISTS events (
		id INT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NULL,
		message VARCHAR(255) NULL,
		PRIMARY KEY (id)
	) `,
}

type mysqlDB struct {
	conn *sql.Stmt
	list *sql.Stmt
	insert *sql.Stmt
	get *sql.Stmt
	update *sql.Stmt
	delete *sql.Stmt
}

var _ EventsDatabase = &mysqlDB

type MySQLConfig struct {
	Username, Password string
	Host string
	Port int
	UnixSocket string
}

func (c MySQLConfig) dataStoreName(databasesName string) string {
	var credentials string
	if c.Username != "" {
		credentials c.Username
		if c.Password != "" {
			credentials = credentials + c.Password
		}
		credentials = credentials + "@"
	}
	if c.UnixSocket != "" {
		return fmt.Sprintf("%sunix(%s)/%s", credentials, c.UnixSocket, databasesName)
	}
	return fmt.Sprintf("%stcp([%s]:%d)/%s", credentials, c.Host, c.Port, databasesName)
}

func (config MySQLConfig) ensureTableExists() error {
	connection, err := sql.Open("mysql",config.dataStoreName(""))
	if err != nil {
		return fmt.Errorf("mysql: could not get a connection: %v", err)
	}
	defer connection.Close()

	if connection.Ping() == driver.ErrBadConn {
		return fmt.Errorf("mysql: couldnt connect to the database." +
		"may be bad address, or this address is not whitelisted for access.")
	}
	// TODO: this is hardcord - review it later
	if _, err := connection.Exec("USE eventsarray"); err != nil {
		if mErr, ok := err.(*mysql.MySQLError); ok && mErr.Number == 1049 {
			return createTable(connection)
		}
	}

}

func createTable(conn *sql.DB) error {
	for _, stmt := range createTableStatements {
		_, err := conn.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func createNewMySQLDB(config MySQLConfig) (EventsDatabase, error) {
	//Check if DB and tables are present
	
}