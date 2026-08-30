package job

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func TestCreateExecutionColumnAndArgumentCountsMatch(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	db := sqlx.NewDb(database, "sqlmock")
	repository := &SQLRepository{db: db}
	now := time.Now()
	value := Execution{ID: "execution-1", JobID: "job-1", TriggerType: "manual", Status: "running", StartedAt: now, Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: "user-1", UpdatedBy: "user-1"}

	query := `INSERT INTO job_executions (` + executionColumns + `) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(value.ID, value.JobID, value.TriggerType, value.Status, value.ResponseJSON, value.ErrorCode, value.ErrorMessage, value.StartedAt, value.FinishedAt, value.DurationMilliseconds, value.Version, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repository.CreateExecution(context.Background(), db, value); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
