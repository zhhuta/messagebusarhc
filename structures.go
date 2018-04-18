package messagebusarhc

// Event base object that we going to pass thought all our services.
type Event struct {
	ID      int64
	Name    string
	Message string
}

// Listing methods of EventDatabase
type EventDatabase interface {
	//get all events
	ListEvents() ([]*Event, error)
	//get aprticular event
	GetEvent(id int64) (*Event, error)
	//add event to the db
	AddEevent(e Event) (id int64, err error)
	// Close connection to db
	Close()
}
