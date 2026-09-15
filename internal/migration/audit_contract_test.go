package migration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSchedulerAuditMigrationCoversEveryTable(t *testing.T) {
	t.Parallel()
	for _, dialect := range []string{"postgres", "kingbase", "mysql"} {
		dialect := dialect
		t.Run(dialect, func(t *testing.T) {
			t.Parallel()
			contents, err := os.ReadFile(filepath.Join("..", "..", "migrations", dialect, "000004_audit_soft_delete.up.sql"))
			if err != nil {
				t.Fatal(err)
			}
			sql := strings.ToLower(string(contents))
			for _, table := range []string{"scheduled_jobs", "job_executions"} {
				if !strings.Contains(sql, "alter table "+table) || !strings.Contains(sql, "deleted_at") || !strings.Contains(sql, "deleted_by") {
					t.Fatalf("%s audit migration does not add logical deletion to %s", dialect, table)
				}
				if dialect != "mysql" && !strings.Contains(sql, "app_enable_audit('"+table+"')") {
					t.Fatalf("%s audit migration does not enable trigger for %s", dialect, table)
				}
			}
		})
	}
}

func TestRepositoryDoesNotPhysicallyDeleteAuditedRows(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile(filepath.Join("..", "job", "repository.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToUpper(string(contents)), "DELETE FROM SCHEDULED_JOBS") || strings.Contains(strings.ToUpper(string(contents)), "DELETE FROM JOB_EXECUTIONS") {
		t.Fatal("runtime repository must use audited logical deletion")
	}
}
