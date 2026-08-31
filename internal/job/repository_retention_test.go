package job

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func TestSQLRepositoryDeleteTerminalExecutionsBefore(t *testing.T) {
	t.Parallel()

	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	repository := NewRepository(sqlx.NewDb(database, "sqlmock"))
	before := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM job_executions WHERE status IN ('succeeded','failed') AND finished_at<? ORDER BY finished_at,id LIMIT ?")).
		WithArgs(before, 2).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("execution-1").AddRow("execution-2"))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM job_executions WHERE id IN (?, ?) AND status IN ('succeeded','failed') AND finished_at<?")).
		WithArgs("execution-1", "execution-2", before).
		WillReturnResult(sqlmock.NewResult(0, 2))

	deleted, err := repository.DeleteTerminalExecutionsBefore(t.Context(), before, 2)
	if err != nil {
		t.Fatalf("DeleteTerminalExecutionsBefore() error = %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
