-- ============================================================
-- Redshift System Tables / Views Mock
-- Creates empty STL tables, STV views, SVV views, SVL objects
-- that applications commonly query for introspection.
-- ============================================================

-- ============================================================
-- STL TABLES (system table log - append-only in real Redshift)
-- Modeled as regular tables; always empty in the emulator.
-- ============================================================

-- stl_query: top-level query log
CREATE TABLE IF NOT EXISTS stl_query (
    userid          INTEGER,
    query           INTEGER,
    label           VARCHAR(320),
    xid             BIGINT,
    pid             INTEGER,
    database        VARCHAR(32),
    querytxt        VARCHAR(4000),
    starttime       TIMESTAMP,
    endtime         TIMESTAMP,
    aborted         INTEGER,
    insert_pristine INTEGER,
    concurrency_scaling_status INTEGER
);

-- stl_querytext: multi-row query text storage (Redshift splits long queries)
CREATE TABLE IF NOT EXISTS stl_querytext (
    userid  INTEGER,
    xid     BIGINT,
    pid     INTEGER,
    query   INTEGER,
    sequence INTEGER,
    text    VARCHAR(200)
);

-- stl_ddltext: DDL statement log
CREATE TABLE IF NOT EXISTS stl_ddltext (
    userid   INTEGER,
    xid      BIGINT,
    pid      INTEGER,
    label    VARCHAR(320),
    starttime TIMESTAMP,
    endtime  TIMESTAMP,
    sequence INTEGER,
    text     VARCHAR(200)
);

-- stl_alert_event_log: query alerts (e.g., hash loop, nested loop join)
CREATE TABLE IF NOT EXISTS stl_alert_event_log (
    userid      INTEGER,
    query       INTEGER,
    slice       INTEGER,
    segment     INTEGER,
    step        INTEGER,
    pid         INTEGER,
    xid         BIGINT,
    event       VARCHAR(512),
    solution    VARCHAR(512),
    event_time  TIMESTAMP
);

-- stl_load_errors: COPY command errors
CREATE TABLE IF NOT EXISTS stl_load_errors (
    userid      INTEGER,
    slice       INTEGER,
    tbl         INTEGER,
    starttime   TIMESTAMP,
    session     INTEGER,
    query       INTEGER,
    filename    VARCHAR(256),
    line_number BIGINT,
    colname     VARCHAR(127),
    type        VARCHAR(10),
    col_length  VARCHAR(10),
    position    INTEGER,
    raw_line    VARCHAR(1024),
    raw_field_value VARCHAR(1024),
    err_code    INTEGER,
    err_reason  VARCHAR(100)
);

-- stl_connection_log: client connections and disconnections
CREATE TABLE IF NOT EXISTS stl_connection_log (
    event       VARCHAR(10),
    recordtime  TIMESTAMP,
    remotehost  VARCHAR(45),
    remoteport  VARCHAR(6),
    pid         INTEGER,
    dbname      VARCHAR(32),
    username    VARCHAR(32),
    authmethod  VARCHAR(32),
    duration    INTEGER,
    sslversion  VARCHAR(64),
    sslcipher   VARCHAR(64),
    mtu         INTEGER,
    sslcompression BOOLEAN,
    sslexpansion   BOOLEAN,
    iamauthtype    VARCHAR(32),
    sessionid      BIGINT,
    external_userid VARCHAR(128),
    userid         INTEGER
);

-- stl_wlm_query: workload manager query info
CREATE TABLE IF NOT EXISTS stl_wlm_query (
    userid              INTEGER,
    xid                 BIGINT,
    task                INTEGER,
    query               INTEGER,
    service_class       INTEGER,
    service_class_name  VARCHAR(64),
    slot_count          INTEGER,
    service_class_start_time TIMESTAMP,
    queue_start_time    TIMESTAMP,
    queue_end_time      TIMESTAMP,
    total_queue_time    BIGINT,
    exec_start_time     TIMESTAMP,
    exec_end_time       TIMESTAMP,
    total_exec_time     BIGINT,
    service_class_end_time TIMESTAMP,
    final_state         VARCHAR(16),
    est_peak_mem        BIGINT
);

-- stl_scan: per-scan statistics
CREATE TABLE IF NOT EXISTS stl_scan (
    userid      INTEGER,
    query       INTEGER,
    slice       INTEGER,
    segment     INTEGER,
    step        INTEGER,
    starttime   TIMESTAMP,
    endtime     TIMESTAMP,
    tasknum     INTEGER,
    rows        BIGINT,
    packets     BIGINT,
    perm_table_name VARCHAR(136),
    is_rrscan   CHAR(1),
    is_delayed_scan CHAR(1),
    rows_pre_filter BIGINT
);

-- ============================================================
-- STV OBJECTS (system table virtual - runtime snapshots)
-- ============================================================

-- stv_inflight: currently running queries (view over pg_stat_activity)
CREATE OR REPLACE VIEW stv_inflight AS
SELECT
    usesysid                            AS userid,
    pid,
    datname                             AS database,
    query                               AS text,
    state_change                        AS starttime,
    NULL::integer                       AS wlm_start_time,
    NULL::integer                       AS label,
    NULL::integer                       AS suspended
FROM pg_stat_activity
WHERE state = 'active';

-- stv_sessions: active sessions
CREATE OR REPLACE VIEW stv_sessions AS
SELECT
    usesysid                AS process,
    usename                 AS user_name,
    datname                 AS db_name,
    pid                     AS pid,
    state_change            AS starttime,
    NULL::varchar(45)       AS remotehost,
    NULL::varchar(6)        AS remoteport,
    application_name        AS label
FROM pg_stat_activity;

-- stv_blocklist: empty (blocked transactions)
CREATE TABLE IF NOT EXISTS stv_blocklist (
    slice       INTEGER,
    col         INTEGER,
    tbl         INTEGER,
    blocknum    INTEGER,
    num_values  INTEGER,
    minvalue    BIGINT,
    maxvalue    BIGINT,
    sb_pos      INTEGER,
    pinned      INTEGER,
    on_disk     INTEGER,
    modified    INTEGER,
    hdr_modified INTEGER
);

-- stv_tbl_perm: permanent table catalog (backed by pg_class)
CREATE OR REPLACE VIEW stv_tbl_perm AS
SELECT
    c.oid                       AS id,
    n.nspname                   AS db,
    c.relname                   AS name,
    c.relpages                  AS pages,
    c.reltuples::bigint         AS rows,
    NULL::integer               AS slice,
    NULL::integer               AS sorted_rows,
    NULL::integer               AS temp
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r';

-- stv_recents: recently completed queries (empty)
CREATE TABLE IF NOT EXISTS stv_recents (
    status      VARCHAR(10),
    starttime   TIMESTAMP,
    duration    BIGINT,
    userid      INTEGER,
    query       VARCHAR(200),
    pid         INTEGER,
    label       VARCHAR(320)
);

-- ============================================================
-- SVV OBJECTS (system view virtual - catalog-level views)
-- ============================================================

-- svv_table_info: table metadata summary
CREATE OR REPLACE VIEW svv_table_info AS
SELECT
    n.nspname                       AS "schema",
    c.relname                       AS "table",
    c.oid                           AS table_id,
    NULL::text                      AS encoded,
    NULL::text                      AS diststyle,
    NULL::text                      AS sortkey1,
    NULL::integer                   AS max_varchar,
    NULL::integer                   AS sortkey1_enc,
    NULL::integer                   AS sortkey_num,
    c.reltuples::bigint             AS size,
    NULL::bigint                    AS pct_used,
    NULL::bigint                    AS unsorted,
    NULL::bigint                    AS stats_off,
    NULL::bigint                    AS tbl_rows,
    NULL::bigint                    AS skew_sortkey1,
    NULL::bigint                    AS skew_rows,
    NULL::text                      AS estimated_visible_rows,
    NULL::text                      AS risk_event,
    NULL::text                      AS vacuum_sort_benefit
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r';

-- svv_columns: unified column info (like information_schema.columns)
CREATE OR REPLACE VIEW svv_columns AS
SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    column_default,
    is_nullable,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    datetime_precision,
    NULL::text AS encoding,
    NULL::text AS distkey,
    NULL::text AS sortkey,
    NULL::text AS column_acl,
    NULL::text AS remarks
FROM information_schema.columns;

-- svv_external_tables: external (Spectrum) tables — empty
CREATE TABLE IF NOT EXISTS svv_external_tables (
    schemaname  VARCHAR(128),
    tablename   VARCHAR(128),
    location    VARCHAR(4096),
    input_format VARCHAR(512),
    output_format VARCHAR(512),
    serialization_lib VARCHAR(512),
    serde_parameters VARCHAR(16384),
    compressed  INTEGER,
    parameters  VARCHAR(16384),
    numrows     BIGINT
);

-- svv_external_columns: external table columns — empty
CREATE TABLE IF NOT EXISTS svv_external_columns (
    schemaname      VARCHAR(128),
    tablename       VARCHAR(128),
    columnname      VARCHAR(128),
    external_type   VARCHAR(200),
    columnnum       INTEGER,
    part_key        INTEGER,
    is_nullable     VARCHAR(10)
);

-- svv_diskusage: disk usage approximation
CREATE OR REPLACE VIEW svv_diskusage AS
SELECT
    n.nspname           AS "schema",
    c.relname           AS "table",
    c.oid               AS tbl,
    c.relpages          AS blocks,
    pg_relation_size(c.oid) AS used_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','i');

-- svv_vacuum_progress: vacuum progress — empty
CREATE TABLE IF NOT EXISTS svv_vacuum_progress (
    table_name  VARCHAR(136),
    status      VARCHAR(50),
    time_remaining_estimate VARCHAR(50)
);

-- svv_transactions: active transactions (backed by pg_stat_activity)
CREATE OR REPLACE VIEW svv_transactions AS
SELECT
    txid_current()          AS txn_owner,
    usename                 AS txn_db,
    pid,
    NULL::timestamp         AS txn_start,
    NULL::integer           AS lock_mode,
    NULL::integer           AS lock_status,
    NULL::integer           AS relation
FROM pg_stat_activity
WHERE backend_type = 'client backend';

-- ============================================================
-- SVL OBJECTS (system view log - historical/audit)
-- ============================================================

-- svl_query_summary: query summary statistics — empty
CREATE TABLE IF NOT EXISTS svl_query_summary (
    userid      INTEGER,
    query       INTEGER,
    stm         INTEGER,
    seg         INTEGER,
    step        INTEGER,
    maxtime     BIGINT,
    avgtime     BIGINT,
    rows        BIGINT,
    bytes       BIGINT,
    rate_row    DOUBLE PRECISION,
    rate_byte   DOUBLE PRECISION,
    label       VARCHAR(256),
    is_rrscan   CHAR(1),
    is_delayed_scan CHAR(1)
);

-- svl_qlog: query log (view over pg_stat_activity for active; historical empty)
CREATE OR REPLACE VIEW svl_qlog AS
SELECT
    usesysid        AS userid,
    pid,
    query           AS query,
    state_change    AS starttime,
    NULL::timestamp AS endtime,
    NULL::integer   AS aborted,
    query           AS queuetime,
    NULL::text      AS label,
    application_name AS service_class,
    NULL::integer   AS slots
FROM pg_stat_activity;

-- svl_query_metrics: per-query metrics — empty
CREATE TABLE IF NOT EXISTS svl_query_metrics (
    userid          INTEGER,
    query           INTEGER,
    service_class   INTEGER,
    segment         INTEGER,
    step_type       INTEGER,
    starttime       TIMESTAMP,
    slices_max      INTEGER,
    slices_total    INTEGER,
    rows_max        BIGINT,
    rows_total      BIGINT,
    rows_std_dev    DOUBLE PRECISION,
    bytes_max       BIGINT,
    bytes_total     BIGINT,
    bytes_std_dev   DOUBLE PRECISION
);
