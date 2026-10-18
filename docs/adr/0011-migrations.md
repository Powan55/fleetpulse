# 0011 Migrations

## Context
Exactly one thing should run schema migrations. The processor can scale to two replicas, and both the api and the processor need the schema in place before they start.

## Options
- The processor runs Flyway on startup. Two replicas race for it, and the api has to wait for the processor.
- A one-shot `migrate` container using the Flyway CLI image. Chosen.

## Decision
- Plain SQL migrations in `db/migrations/V<n>__<desc>.sql`; their end state is `contracts/db-schema.sql`. Applied migrations are never edited.
- `migrate` runs `flyway/flyway:13.10.3` against `/migrations`, connects as the Postgres superuser that owns the tables, takes passwords from environment variables (not the command line), retries the connection, then exits. The processor and api start only after it completes successfully.
- The services connect as separate non-superuser roles: one for the processor, one for the api (limited writes), one read-only role for reconciliation scripts. No service has Flyway on its runtime classpath.
- Integration tests apply the same SQL files with the Flyway Java API against a Testcontainers Postgres, then connect as the runtime role under test.

## Consequences
- A failed migration stops the stack before any consumer starts.
- Passwords are still visible in `docker inspect`. They are local-only defaults.
