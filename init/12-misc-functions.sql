-- ============================================================
-- Redshift Miscellaneous Functions Emulation
-- Functions not covered by previous init files.
-- ============================================================

-- ------------------------------------------------------------
-- APPROXIMATE PERCENTILE_DISC passthrough note:
-- Redshift supports APPROXIMATE PERCENTILE_DISC as a fast
-- approximate version of PERCENTILE_DISC.  PostgreSQL does not
-- have an APPROXIMATE keyword; we expose the exact ordered-set
-- aggregate PERCENTILE_DISC as a compatibility shim.
--
-- Usage in app code that originally used:
--   APPROXIMATE PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY col)
-- should be rewritten to:
--   PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY col)
-- which is valid standard SQL and works natively in PostgreSQL.
-- No wrapper function needed — standard syntax works as-is.
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- CURRENT_USER_ID() → INTEGER
-- Returns the OID of the current database user (pg_roles.oid).
-- In Redshift this returns an integer userid.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION CURRENT_USER_ID()
RETURNS INTEGER
LANGUAGE SQL
STABLE
AS $$
  SELECT (SELECT oid FROM pg_roles WHERE rolname = current_user)::integer;
$$;

-- ------------------------------------------------------------
-- CURRENT_DATABASE() is a built-in in PostgreSQL.
-- No wrapper needed, but alias for Redshift compatibility:
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- CURRENT_SCHEMA() is also a built-in. No wrapper needed.
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- PG_LAST_QUERY_ID() → INTEGER
-- Redshift returns the query id of the last query run by the
-- current session.  In PostgreSQL there is no direct equivalent;
-- return -1 as a harmless placeholder.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION PG_LAST_QUERY_ID()
RETURNS INTEGER
LANGUAGE SQL
VOLATILE
AS $$
  SELECT -1;
$$;

-- ------------------------------------------------------------
-- PG_LAST_COPY_ID() → INTEGER
-- Redshift-only; returns -1 as placeholder.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION PG_LAST_COPY_ID()
RETURNS INTEGER
LANGUAGE SQL
VOLATILE
AS $$
  SELECT -1;
$$;

-- ------------------------------------------------------------
-- PG_LAST_UNLOAD_ID() → INTEGER
-- Redshift-only; returns -1 as placeholder.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION PG_LAST_UNLOAD_ID()
RETURNS INTEGER
LANGUAGE SQL
VOLATILE
AS $$
  SELECT -1;
$$;

-- ------------------------------------------------------------
-- SLICE_NUM() → INTEGER
-- Returns the compute node slice number.  Always 0 on single-
-- node PostgreSQL emulator.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION SLICE_NUM()
RETURNS INTEGER
LANGUAGE SQL
STABLE
AS $$
  SELECT 0;
$$;

-- ------------------------------------------------------------
-- CURRENT_SLICE() → INTEGER (alias)
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION CURRENT_SLICE()
RETURNS INTEGER
LANGUAGE SQL
STABLE
AS $$
  SELECT 0;
$$;

-- ------------------------------------------------------------
-- NODE_ID() → INTEGER
-- Returns the compute node id.  Always 0 in emulator.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION NODE_ID()
RETURNS INTEGER
LANGUAGE SQL
STABLE
AS $$
  SELECT 0;
$$;

-- ------------------------------------------------------------
-- APPROXIMATE COUNT(DISTINCT ...) is not a UDF — it is a query
-- syntax variant.  In this emulator COUNT(DISTINCT ...) returns
-- the exact count, which is a safe superset of the approximate.
-- No wrapper needed.
-- ------------------------------------------------------------
