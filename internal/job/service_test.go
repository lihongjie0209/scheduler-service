package job

import (
	"context"
	"errors"
	"testing"

	"github.com/lihongjie0209/scheduler-service/internal/apperror"
)

type fakeInvoker struct{ err error }

func (f fakeInvoker) Validate(context.Context, string, string, string) error { return f.err }
func (f fakeInvoker) Invoke(context.Context, string, string, string) (string, error) {
	return `{}`, f.err
}

func TestNormalizeAndValidate(t *testing.T) {
	t.Parallel()
	valid := Input{Name: "daily report", CronExpression: "0 0 2 * * *", Upstream: "reporting", FullMethod: "/platform.reporting.v1.Reporting/Generate", RequestJSON: `{}`, TimeoutMilliseconds: 5000, Enabled: true}
	for _, test := range []struct {
		name      string
		mutate    func(*Input)
		invoker   Invoker
		wantError bool
	}{
		{name: "valid defaults timezone", invoker: fakeInvoker{}},
		{name: "invalid cron", mutate: func(value *Input) { value.CronExpression = "invalid" }, invoker: fakeInvoker{}, wantError: true},
		{name: "invalid timeout", mutate: func(value *Input) { value.TimeoutMilliseconds = 99 }, invoker: fakeInvoker{}, wantError: true},
		{name: "reflection validation", invoker: fakeInvoker{err: errors.New("method missing")}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := valid
			if test.mutate != nil {
				test.mutate(&value)
			}
			got, err := normalizeAndValidate(t.Context(), test.invoker, value)
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v", err)
			}
			if err == nil && got.Timezone != "Asia/Shanghai" {
				t.Fatalf("timezone = %q", got.Timezone)
			}
		})
	}
}

func TestPagination(t *testing.T) {
	t.Parallel()
	page, size, err := pagination(0, 0)
	if err != nil || page != 1 || size != 20 {
		t.Fatalf("pagination = %d,%d,%v", page, size, err)
	}
	_, _, err = pagination(1, 101)
	var appErr *apperror.Error
	if !errors.As(err, &appErr) || appErr.Code != apperror.CodeInvalidArgument {
		t.Fatalf("error = %#v", err)
	}
}

func TestStatusFromEnabled(t *testing.T) {
	t.Parallel()
	if statusFromEnabled(true) != "enabled" || statusFromEnabled(false) != "disabled" {
		t.Fatal("unexpected status mapping")
	}
}
