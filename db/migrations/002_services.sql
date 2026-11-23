CREATE TABLE IF NOT EXISTS information_systems (
    id text PRIMARY KEY,
    name text NOT NULL,
    description text
);

CREATE TABLE IF NOT EXISTS servers (
    id text PRIMARY KEY,
    hostname text NOT NULL,
    is_id text NOT NULL REFERENCES information_systems(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz
);

CREATE TABLE IF NOT EXISTS server_thresholds (
    server_id text PRIMARY KEY REFERENCES servers(id),
    cpu_percent double precision NOT NULL CHECK (cpu_percent BETWEEN 1 AND 100),
    ram_percent double precision NOT NULL CHECK (ram_percent BETWEEN 1 AND 100),
    disk_percent double precision NOT NULL CHECK (disk_percent BETWEEN 1 AND 100)
);

CREATE TABLE IF NOT EXISTS server_live_metrics (
    server_id text NOT NULL,
    measured_at timestamptz NOT NULL,
    cpu_percent double precision NOT NULL CHECK (cpu_percent BETWEEN 0 AND 100),
    ram_percent double precision NOT NULL CHECK (ram_percent BETWEEN 0 AND 100),
    disk_percent double precision NOT NULL CHECK (disk_percent BETWEEN 0 AND 100),
    PRIMARY KEY (server_id, measured_at)
);

SELECT create_hypertable('server_live_metrics', by_range('measured_at'), if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS server_forecasts (
    server_id text NOT NULL REFERENCES servers(id),
    horizon text NOT NULL CHECK (horizon IN ('1d', '1m', '6m', '1y')),
    mode text NOT NULL CHECK (mode IN ('mean', 'peak')),
    cpu_percent double precision NOT NULL,
    ram_percent double precision NOT NULL,
    disk_percent double precision NOT NULL,
    generated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (server_id, horizon, mode)
);

CREATE TABLE IF NOT EXISTS alert_events (
    id uuid PRIMARY KEY,
    server_id text NOT NULL REFERENCES servers(id),
    horizon text NOT NULL,
    metric text NOT NULL,
    forecast double precision NOT NULL,
    threshold double precision NOT NULL,
    opened_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    email_done boolean NOT NULL DEFAULT false,
    push_done boolean NOT NULL DEFAULT false,
    email_attempts integer NOT NULL DEFAULT 0,
    push_attempts integer NOT NULL DEFAULT 0,
    email_retry_at timestamptz,
    push_retry_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS active_alert_event
    ON alert_events (server_id, horizon, metric) WHERE resolved_at IS NULL;
