package db

import (
	"database/sql"
	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"sync"
	"time"

	"git.neds.sh/matty/entain/event/proto/event"
)

// EventsRepo provides repository access to events.
type EventsRepo interface {
	// Init will initialise our events repository.
	Init() error

	// List will return a list of events.
	List(filter *event.ListEventsRequestFilter) ([]*event.SportEvent, error)
}

type eventsRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewEventsRepo creates a new events repository.
func NewEventsRepo(db *sql.DB) EventsRepo {
	return &eventsRepo{db: db}
}

// Init prepares the event repository dummy data.
func (r *eventsRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy events.
		err = r.seed()
	})

	return err
}

func (r *eventsRepo) List(filter *event.ListEventsRequestFilter) ([]*event.SportEvent, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getEventQueries()[eventList]

	query, args = r.applyFilter(query, filter)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanEvents(rows)
}

func (r *eventsRepo) applyFilter(query string, filter *event.ListEventsRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}

	if len(filter.EventIds) > 0 {
		clauses = append(clauses, "id IN ("+strings.Repeat("?,", len(filter.EventIds)-1)+"?)")

		for _, eventID := range filter.EventIds {
			args = append(args, eventID)
		}
	}

	// if no visible status received, do nothing and return all events
	switch filter.VisibleStatus {
	case event.EventVisibleStatus_EVENT_VISIBLE_STATUS_VISIBLE:
		clauses = append(clauses, "visible = 1")
	case event.EventVisibleStatus_EVENT_VISIBLE_STATUS_INVISIBLE:
		clauses = append(clauses, " visible = 0")
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	// if no sort by field received, sort by advertised_start_time
	// ASC/DESC is not considered in this scenario
	switch filter.SortBy {
	case event.EventSortByField_EVENT_SORT_BY_FIELD_ID:
		query += " ORDER BY id"
	case event.EventSortByField_EVENT_SORT_BY_FIELD_NAME:
		query += " ORDER BY name"
	case event.EventSortByField_EVENT_SORT_BY_FIELD_VISIBLE:
		query += " ORDER BY visible"
	default:
		query += " ORDER BY advertised_start_time"
	}

	return query, args
}

func (m *eventsRepo) scanEvents(
	rows *sql.Rows,
) ([]*event.SportEvent, error) {
	var events []*event.SportEvent

	for rows.Next() {
		var event event.SportEvent
		var advertisedStart time.Time

		if err := rows.Scan(&event.Id, &event.Name, &event.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		event.AdvertisedStartTime = ts

		events = append(events, &event)
	}

	return events, nil
}
