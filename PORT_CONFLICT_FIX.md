# Port Conflict Resolution

## Problem
A local PostgreSQL instance is running on port 5432, preventing Docker containers from being accessible.

## Detection
```bash
lsof -i :5432
```

If you see a `postgres` process that's not from Docker, you have a conflict.

## Solution Options

### Option 1: Stop Local PostgreSQL (Recommended)

If you're not using the local PostgreSQL:

**macOS (Homebrew):**
```bash
brew services stop postgresql
```

**macOS (pg_ctl):**
```bash
pg_ctl -D /usr/local/var/postgres stop
```

**Linux (systemd):**
```bash
sudo systemctl stop postgresql
```

Then restart Docker containers:
```bash
docker-compose down
docker-compose up -d
```

### Option 2: Use Different Ports for Docker

If you need to keep local PostgreSQL running, change Docker ports in `docker-compose.yml`:

```yaml
version: '3.8'
services:
  pg_source:
    image: postgres:16
    command: postgres -c wal_level=logical
    environment:
      POSTGRES_PASSWORD: password
      POSTGRES_DB: source_db
    ports:
      - "15432:5432"  # Changed from 5432:5432
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  pg_sink:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: password
      POSTGRES_DB: sink_db
    ports:
      - "15433:5432"  # Changed from 5433:5432
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  clickhouse:
    image: clickhouse/clickhouse-server
    ports:
      - "8123:8123"
      - "9000:9000"
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
```

Then update `config.yaml`:

```yaml
source:
  connection_string: "postgres://postgres:password@127.0.0.1:15432/source_db?replication=database&sslmode=disable"
  slot_name: "replicator_slot"
  publication: "my_pub"

targets:
  postgres:
    - name: "pg_main"
      connection_string: "postgres://postgres:password@127.0.0.1:15433/sink_db?sslmode=disable"
      # ... rest of config
```

And restart:
```bash
docker-compose down
docker-compose up -d
make e2e-test
```

## Verification

After applying the fix, verify:

```bash
# Check no local postgres on 5432
lsof -i :5432 | grep postgres

# Should only see Docker processes now
docker ps | grep postgres
```
