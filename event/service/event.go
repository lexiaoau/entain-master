package service

import (
	"git.neds.sh/matty/entain/event/db"
	"git.neds.sh/matty/entain/event/proto/event"
	"golang.org/x/net/context"
)

type SportEvent interface {
	// ListEvents will return a collection of events.
	ListEvents(ctx context.Context, in *event.ListEventsRequest) (*event.ListEventsResponse, error)
}

// eventService implements the event interface.
type eventService struct {
	eventsRepo db.EventsRepo
}

// NewEventService instantiates and returns a new eventService.
func NewEventService(eventsRepo db.EventsRepo) SportEvent {
	return &eventService{eventsRepo}
}

func (s *eventService) ListEvents(ctx context.Context, in *event.ListEventsRequest) (*event.ListEventsResponse, error) {
	events, err := s.eventsRepo.List(in.Filter)
	if err != nil {
		return nil, err
	}

	return &event.ListEventsResponse{Events: events}, nil
}
