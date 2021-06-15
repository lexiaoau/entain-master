package db

import (
	"database/sql"
	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"sync"
	"time"

	"git.neds.sh/matty/entain/racing/proto/racing"
)

// RacesRepo provides repository access to races.
type RacesRepo interface {
	// Init will initialise our races repository.
	Init() error

	// List will return a list of races.
	List(filter *racing.ListRacesRequestFilter) ([]*racing.Race, error)

	// Get single race by its ID
	GetRaceByID(ID string) (*racing.Race, error)
}

type racesRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewRacesRepo creates a new races repository.
func NewRacesRepo(db *sql.DB) RacesRepo {
	return &racesRepo{db: db}
}

// Init prepares the race repository dummy data.
func (r *racesRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy races.
		err = r.seed()
	})

	return err
}

func (r *racesRepo) List(filter *racing.ListRacesRequestFilter) ([]*racing.Race, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getRaceQueries()[racesList]

	query, args = r.applyFilter(query, filter)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanRaces(rows)
}

func (r *racesRepo) applyFilter(query string, filter *racing.ListRacesRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}

	if len(filter.MeetingIds) > 0 {
		clauses = append(clauses, "meeting_id IN ("+strings.Repeat("?,", len(filter.MeetingIds)-1)+"?)")

		for _, meetingID := range filter.MeetingIds {
			args = append(args, meetingID)
		}
	}

	// if no visible status received, do nothing and return all races
	switch filter.VisibleStatus {
	case racing.RaceVisibleStatus_RACE_VISIBLE_STATUS_VISIBLE:
		clauses = append(clauses, "visible = 1")
	case racing.RaceVisibleStatus_RACE_VISIBLE_STATUS_INVISIBLE:
		clauses = append(clauses, " visible = 0")
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	// if no sort by field received, sort by advertised_start_time
	// ASC/DESC is not considered in this scenario
	switch filter.SortBy {
	case racing.SortByField_SORT_BY_FIELD_ID:
		query += " ORDER BY id"
	case racing.SortByField_SORT_BY_FIELD_MEETING_ID:
		query += " ORDER BY meeting_id"
	case racing.SortByField_SORT_BY_FIELD_NAME:
		query += " ORDER BY name"
	case racing.SortByField_SORT_BY_FIELD_NUMBER:
		query += " ORDER BY number"
	case racing.SortByField_SORT_BY_FIELD_VISIBLE:
		query += " ORDER BY visible"
	default:
		query += " ORDER BY advertised_start_time"
	}

	return query, args
}

func (m *racesRepo) scanRaces(
	rows *sql.Rows,
) ([]*racing.Race, error) {
	var races []*racing.Race

	currentTime := time.Now()

	for rows.Next() {
		var race racing.Race
		var advertisedStart time.Time

		if err := rows.Scan(&race.Id, &race.MeetingId, &race.Name, &race.Number, &race.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		race.AdvertisedStartTime = ts

		if currentTime.After(ts.AsTime()) {
			race.Status = racing.RaceStatus_RACE_STATUS_CLOSED
		} else {
			race.Status = racing.RaceStatus_RACE_STATUS_OPEN
		}

		races = append(races, &race)
	}

	return races, nil
}

func (r *racesRepo) GetRaceByID(id string) (*racing.Race, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getRaceQueries()[racesList]

	query += " WHERE id = " + id

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	scannedRows, err := r.scanRaces(rows)
	if err != nil {
		return nil, err
	}

	if len(scannedRows) == 0 {
		return nil, nil
	}

	return scannedRows[0], nil
}
