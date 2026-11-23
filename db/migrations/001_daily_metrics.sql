CREATE TABLE server_daily_metrics (
    server_id text,
    day date,
    cpu_percent float,
    ram_percent float,
    disk_percent float,
    PRIMARY KEY (server_id, day)
);