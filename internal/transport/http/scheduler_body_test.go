package httptransport

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/lihongjie0209/scheduler-service/internal/job"
)

func TestJobBody_PublicJSONContract(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 2, 8, 0, 0, 0, time.UTC)
	encoded, err := json.Marshal(jobBody(job.Job{
		ID: "job-1", TenantID: "tenant-1", ApplicationID: "app-1", Name: "reconcile",
		CronExpression: "0 * * * *", Timezone: "Asia/Shanghai", Upstream: "billing-service",
		FullMethod: "/platform.billing.v1.BillingService/Reconcile", RequestJSON: `{}`,
		TimeoutMilliseconds: 5000, Status: "active", Version: 2, CreatedAt: now, UpdatedAt: now,
		CreatedBy: "user-1", UpdatedBy: "user-2",
	}))
	if err != nil {
		t.Fatalf("marshal job body: %v", err)
	}

	assertJSONKeys(t, encoded, []string{
		"application_id", "created_at", "created_by", "cron_expression", "full_method", "id", "name",
		"request_json", "status", "tenant_id", "timeout_milliseconds", "timezone", "updated_at", "updated_by",
		"upstream", "version",
	})
}

func TestExecutionBody_PublicJSONContract(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 2, 8, 0, 0, 0, time.UTC)
	encoded, err := json.Marshal(executionBody(job.Execution{
		ID: "execution-1", JobID: "job-1", TenantID: "tenant-1", ApplicationID: "app-1",
		TriggerType: "manual", Status: "succeeded", ResponseJSON: `{}`, StartedAt: now, FinishedAt: &now,
		DurationMilliseconds: 12, Version: 1, CreatedAt: now, UpdatedAt: now,
		CreatedBy: "user-1", UpdatedBy: "user-1",
	}))
	if err != nil {
		t.Fatalf("marshal execution body: %v", err)
	}

	assertJSONKeys(t, encoded, []string{
		"application_id", "created_at", "created_by", "duration_milliseconds", "error_code", "error_message",
		"finished_at", "id", "job_id", "response_json", "started_at", "status", "tenant_id", "trigger_type",
		"updated_at", "updated_by", "version",
	})
}

func assertJSONKeys(t *testing.T, encoded []byte, expected []string) {
	t.Helper()

	var document map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatalf("unmarshal response body: %v", err)
	}
	actual := make([]string, 0, len(document))
	for key := range document {
		actual = append(actual, key)
	}
	sort.Strings(actual)
	sort.Strings(expected)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("public json keys = %v, want %v", actual, expected)
	}
}
