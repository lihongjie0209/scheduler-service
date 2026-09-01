# Migrations

Database-specific migrations live under `mysql`, `postgres`, and `kingbase`. Set `migration.path` to the matching directory, for example `migrations/postgres`. Review indexes, collation and online-DDL impact against production data before deployment.

Migration `000003` introduces tenant/application ownership. Existing jobs have no authoritative scope, so the migration disables them and leaves both scope fields empty; they must be reassigned through an operator-reviewed data migration before execution. Runtime scheduling intentionally ignores unscoped legacy rows.
