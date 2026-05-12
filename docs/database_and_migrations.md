# Database and migrations

See also: [setup_and_configuration.md](setup_and_configuration.md) · [data_pipeline.md](data_pipeline.md)

## Postgres role

Staging lands in **`public.metadata`** and **`public.reviews`**.

## Container init

`infra/docker/initdb/010_create_application_database.sh`:

1. Creates role `APP_DB_USER` with `POSTGRES_PASSWORD` if missing.
2. Creates database `APP_DB_NAME` owned by that role.
3. Finalizes initialization in that database.

Align `PGUSER`, `PGDATABASE`, and `secrets/.psql.pass` with this setup.

## Migration layout

Path: **`migrations/versions/<version_id>/`**

| Artifact | Purpose |
|----------|---------|
| `deploy/*.sql` | Ordered SQL files |
| `revert.sql` | Used by `db/revert_last_migration.py` |
| `verify.sql` | Checked by `make verify-migrations` |

Do not edit `deploy/` for a version already applied on a shared DB; add a new `002_*` version instead.

## Ledger

Schema **`pipeline`**, table **`pipeline.schema_migrations`**: `version` primary key, `applied_at`. Created in `lib/schema_migrations.py`.

## Scripts

| Script | Role |
|--------|------|
| `db/apply_migrations.py` | Apply pending versions |
| `db/verify_migrations.py` | Run each applied version’s `verify.sql` |
| `db/revert_last_migration.py` | Revert latest version; needs `CONFIRM=yes` |
| `db/load_into_postgres.py` | Migrations, TRUNCATE, COPY from `data/staging/` |

## Connection

`lib/dbutil.py` reads `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE` and the first line of `secrets/.psql.pass`.

## Reference DDL

`reference/schema/` is read-only documentation. Executable DDL lives under `migrations/versions/*/deploy/`.
