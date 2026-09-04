package job

import (
	"context"
	"errors"
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
	value := Execution{ID: "execution-1", JobID: "job-1", TenantID: "tenant-1", ApplicationID: "application-1", TriggerType: "manual", Status: "running", StartedAt: now, Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: "user-1", UpdatedBy: "user-1"}

	query := `INSERT INTO job_executions (` + executionColumns + `) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(value.ID, value.JobID, value.TenantID, value.ApplicationID, value.TriggerType, value.Status, value.ResponseJSON, value.ErrorCode, value.ErrorMessage, value.StartedAt, value.FinishedAt, value.DurationMilliseconds, value.Version, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repository.CreateExecution(context.Background(), db, value); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateManualExecutionChecksVersionAndEnabledState(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	db := sqlx.NewDb(database, "sqlmock")
	repository := &SQLRepository{db: db}
	now := time.Now()
	value := Execution{ID: "execution-1", JobID: "job-1", TenantID: "tenant-1", ApplicationID: "application-1", TriggerType: "manual", Status: "running", StartedAt: now, Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: "user-1", UpdatedBy: "user-1"}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT version,status FROM scheduled_jobs WHERE id=? AND status<>'deleted' FOR UPDATE`)).
		WithArgs(value.JobID).
		WillReturnRows(sqlmock.NewRows([]string{"version", "status"}).AddRow(3, "enabled"))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO job_executions (`+executionColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)).
		WithArgs(value.ID, value.JobID, value.TenantID, value.ApplicationID, value.TriggerType, value.Status, value.ResponseJSON, value.ErrorCode, value.ErrorMessage, value.StartedAt, value.FinishedAt, value.DurationMilliseconds, value.Version, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repository.CreateManualExecution(t.Context(), db, value, 3); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateManualExecutionRejectsStaleOrDisabledJob(t *testing.T) {
	for _, test := range []struct {
		name    string
		version int64
		status  string
	}{
		{name: "stale version", version: 4, status: "enabled"},
		{name: "disabled job", version: 3, status: "disabled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			database, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			db := sqlx.NewDb(database, "sqlmock")
			repository := &SQLRepository{db: db}
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT version,status FROM scheduled_jobs WHERE id=? AND status<>'deleted' FOR UPDATE`)).
				WithArgs("job-1").
				WillReturnRows(sqlmock.NewRows([]string{"version", "status"}).AddRow(test.version, test.status))

			err = repository.CreateManualExecution(t.Context(), db, Execution{JobID: "job-1"}, 3)
			if !errors.Is(err, ErrStaleVersion) {
				t.Fatalf("CreateManualExecution() error = %v, want ErrStaleVersion", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
