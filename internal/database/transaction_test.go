package database

import (
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/microservice-platform-go/principal"
)

func TestWithinRequiresAndInjectsAuditActor(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	transactor := NewTransactor(sqlx.NewDb(database, "pgx"))

	mock.ExpectBegin()
	mock.ExpectRollback()
	if err := transactor.Within(t.Context(), nil, func(*sqlx.Tx) error { return nil }); !errors.Is(err, ErrMissingAuditActor) {
		t.Fatalf("Within() error = %v, want ErrMissingAuditActor", err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SELECT set_config('app.actor_id', $1, true)`)).WithArgs("scheduler-service").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	ctx := principal.WithContext(t.Context(), principal.Principal{ID: "scheduler-service", Type: principal.TypeSystem})
	if err := transactor.Within(ctx, nil, func(*sqlx.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
