-- Airflow keeps its own metadata (DAG runs, task instances, connections) in a
-- separate database from the application tables in schema.sql. Both live in
-- the same Postgres container to keep the stack small.
--
-- Named 000- so the Postgres entrypoint runs it before 001-schema.sql.
-- Init scripts only run when the data directory is first created, so if you
-- already have a postgres-data volume, create the database by hand instead:
--   docker exec postgres psql -U hospital -d hospital -c "CREATE DATABASE airflow"
CREATE DATABASE airflow;
