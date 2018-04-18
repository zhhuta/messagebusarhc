package messagebusarhc

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

var createTableStatements = []string{
	`CREATE DATABASES IF NOT EXISTS eventsarray EFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE 'utf8_general_ci';`,
	`USE eventsarray;`,
	`CREATE TABL IF NOT EXISTS events (
		id INT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NULL,
		message VARCHAR(255) NULL,
		PRIMARY KEY (id)
	) `,
}

type mysqlDB struct {
	conn *sql.DB

	list   *sql.Stmt
	insert *sql.Stmt
	get    *sql.Stmt
	update *sql.Stmt
	delete *sql.Stmt
}

// Mapping EventDatabase to mysqlSDB
var _ EventsDatabase = &mysqlDB{}

//Structure that keeps mysql conneciton configuration
type MySQLConfig struct {
	Username   string
	Password   string
	Host       string
	Port       int
	UnixSocket string
}

func (c MySQLConfig) dataStoreName(databasesName string) string {
	var credentials string
	if c.Username != "" {
		credentials = c.Username
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
	connection, err := sql.Open("mysql", config.dataStoreName(""))
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

	if _, err := connection.Exec("DESCRIBE events"); err != nil {
		// MySQL error 1146 is "table does not exist"
		if mErr, ok := err.(*mysql.MySQLError); ok && mErr.Number == 1146 {
			return createTable(connection)
		}
		// Unknown error.
		return fmt.Errorf("mysql: could not connect to the database: %v", err)
	}
	return nil

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
	if err := config.ensureTableExists(); err != nil {
		return nil, err
	}

	conn, err := sql.Open("mysql", config.dataStoreName("eventsarray"))
	if err != nil {
		return nil, fmt.Errorf("mysql: could not get a connection: %v", err)
	}
	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("mysql: could not establish a good connection: %v", err)
	}

	db := &mysqlDB{
		conn: conn,
	}
	if db.list, err = conn.Prepare(listStatement); err != nil {
		return nil, fmt.Errorf("mysql: prepare list: %v", err)
	}

	if db.get, err = conn.Prepare(getStatement); err != nil {
		return nil, fmt.Errorf("mysql: prepare get: %v", err)
	}
	if db.insert, err = conn.Prepare(insertStatement); err != nil {
		return nil, fmt.Errorf("mysql: prepare insert: %v", err)
	}
	if db.update, err = conn.Prepare(updateStatement); err != nil {
		return nil, fmt.Errorf("mysql: prepare update: %v", err)
	}
	if db.delete, err = conn.Prepare(deleteStatement); err != nil {
		return nil, fmt.Errorf("mysql: prepare delete: %v", err)
	}

	return db, nil
}

func (db *mysqlDB) Close() {
	db.conn.Close()
}

// rowScanner is implemented by sql.Row and sql.Rows
type rowScanner interface {
	Scan(dest ...interface{}) error
}

// scanBook reads a book from a sql.Row or sql.Rows
func scanEvent(s rowScanner) (*Event, error) {
	var (
		id      int64
		name    sql.NullString
		message sql.NullString
	)
	if err := s.Scan(&id, &name, &message); err != nil {
		return nil, err
	}

	book := &Event{
		ID:      id,
		Name:    name.String,
		Message: message.String,
	}
	return book, nil
}

// method to Get all records from DB ordered by name
const listStatement = `SELECT * FROM events ORDER BY name`

func (db *mysqlDB) ListEvents() ([]*Event, error) {
	rows, err := db.list.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("mysql: could not read row: %v", err)
		}

		events = append(events, event)
	}

	return events, nil
}

// Method to Get a specific event by id.
const getStatement = "SELECT * FROM books WHERE id = ?"

func (db *mysqlDB) GetEvent(id int64) (*Event, error) {
	event, err := scanEvent(db.get.QueryRow(id))
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("mysql: could not find event with id %d", id)
	}
	if err != nil {
		return nil, fmt.Errorf("mysql: could not get event: %v", err)
	}
	return event, nil
}

const insertStatement = `
 INSERT INTO events (
   name, message, 
 ) VALUES (?, ?)`

func (db *mysqlDB) AddEevent(e Event) (id int64, err error) {
	// emplement  insert.
	return 0, nil
}

const updateStatement = ``

const deleteStatement = ``
