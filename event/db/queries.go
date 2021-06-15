package db

const (
	eventList = "list"
)

func getEventQueries() map[string]string {
	return map[string]string{
		eventList: `
			SELECT 
				id, 
				name, 
				visible, 
				advertised_start_time 
			FROM events
		`,
	}
}
