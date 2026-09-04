package job

import (
	"context"
	"errors"
	"testing"

	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	"github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/scheduler-service/internal/apperror"
)

type fakeInvoker struct{ err error }

type fakeApplicationVerifier struct{ err error }

func (f fakeApplicationVerifier) Verify(context.Context, string, string) error { return f.err }

type trackingInvoker struct{ validateCalls int }

func (f *trackingInvoker) Validate(context.Context, string, string, string) error {
	f.validateCalls++
	return nil
}
func (f *trackingInvoker) Invoke(context.Context, string, string, string) (string, error) {
	return `{}`, nil
}

func (f fakeInvoker) Validate(context.Context, string, string, string) error { return f.err }
func (f fakeInvoker) Invoke(context.Context, string, string, string) (string, error) {
	return `{}`, f.err
}

func TestNormalizeAndValidate(t *testing.T) {
	t.Parallel()
	valid := Input{TenantID: "tenant-1", ApplicationID: "application-1", Name: "daily report", CronExpression: "0 0 2 * * *", Upstream: "reporting", FullMethod: "/platform.reporting.v1.Reporting/Generate", RequestJSON: `{}`, TimeoutMilliseconds: 5000, Enabled: true}
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

func TestValidateManualTrigger(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		job      Job
		expected int64
		wantCode int
	}{
		{name: "enabled current version", job: Job{Status: "enabled", Version: 3}, expected: 3},
		{name: "stale version", job: Job{Status: "enabled", Version: 4}, expected: 3, wantCode: apperror.CodeConflict},
		{name: "disabled job", job: Job{Status: "disabled", Version: 3}, expected: 3, wantCode: apperror.CodeConflict},
		{name: "deleted job", job: Job{Status: "deleted", Version: 3}, expected: 3, wantCode: apperror.CodeConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validateManualTrigger(test.job, test.expected)
			if test.wantCode == 0 {
				if err != nil {
					t.Fatalf("validateManualTrigger() error = %v", err)
				}
				return
			}
			var appErr *apperror.Error
			if !errors.As(err, &appErr) || appErr.Code != test.wantCode {
				t.Fatalf("validateManualTrigger() error = %v, want code %d", err, test.wantCode)
			}
		})
	}
}

func TestTriggerRejectsMissingExpectedVersionBeforeDependencies(t *testing.T) {
	t.Parallel()

	service := NewService(nil, nil, nil, nil)
	_, err := service.Trigger(t.Context(), "job-1", 0)
	var appErr *apperror.Error
	if !errors.As(err, &appErr) || appErr.Code != apperror.CodeInvalidArgument {
		t.Fatalf("Trigger() error = %v, want invalid argument", err)
	}
}

func TestAuthorizeScopeEnforcesTenantAndApplicationGrant(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		tenantID string
		appID    string
		verifier error
		wantCode int
	}{
		{name: "granted", tenantID: "tenant-1", appID: "application-1"},
		{name: "different tenant", tenantID: "tenant-2", appID: "application-1", wantCode: apperror.CodeForbidden},
		{name: "missing scope", tenantID: "tenant-1", wantCode: apperror.CodeInvalidArgument},
		{name: "application denied", tenantID: "tenant-1", appID: "application-1", verifier: appaccess.ErrNotGranted, wantCode: apperror.CodeForbidden},
		{name: "application unavailable", tenantID: "tenant-1", appID: "application-1", verifier: errors.New("unavailable"), wantCode: apperror.CodeDependencyUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := NewService(nil, nil, nil, nil)
			service.applications = fakeApplicationVerifier{err: test.verifier}
			ctx := principal.WithContext(t.Context(), principal.Principal{ID: "user-1", Type: principal.TypeUser, TenantID: "tenant-1"})
			err := service.authorizeScope(ctx, test.tenantID, test.appID)
			if test.wantCode == 0 {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var appErr *apperror.Error
			if !errors.As(err, &appErr) || appErr.Code != test.wantCode {
				t.Fatalf("authorizeScope() error = %v, want code %d", err, test.wantCode)
			}
		})
	}
}

func TestCreateAuthorizesScopeBeforeInspectingDynamicTarget(t *testing.T) {
	t.Parallel()

	invoker := &trackingInvoker{}
	service := NewService(nil, nil, invoker, nil)
	service.applications = fakeApplicationVerifier{err: appaccess.ErrNotGranted}
	ctx := principal.WithContext(t.Context(), principal.Principal{ID: "user-1", Type: principal.TypeUser, TenantID: "tenant-1"})
	_, err := service.Create(ctx, Input{TenantID: "tenant-1", ApplicationID: "application-1"})
	var appErr *apperror.Error
	if !errors.As(err, &appErr) || appErr.Code != apperror.CodeForbidden {
		t.Fatalf("Create() error = %v", err)
	}
	if invoker.validateCalls != 0 {
		t.Fatalf("dynamic target inspected %d times before authorization", invoker.validateCalls)
	}
}
