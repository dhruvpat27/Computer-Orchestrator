package main

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.TaskName, &j.Payload, &j.Status, &j.RetriesLeft,
		&j.MaxRetries, &j.Attempt, &j.Result, &j.Error, &j.WorkerID,
		&j.WorkflowID, &j.DependsOn, &j.CreatedAt, &j.UpdatedAt)
	return j, err
}
