-- 02-date-functions.sql: Redshift date/time function emulation

-- ---------------------------------------------------------------------------
-- 5.2 GETDATE() → current timestamp without time zone
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION getdate()
    RETURNS timestamp
    LANGUAGE sql
    STABLE
AS $$
    SELECT NOW()::timestamp;
$$;

-- ---------------------------------------------------------------------------
-- 5.3 DATEADD(datepart text, count integer, ts timestamp) → timestamp
-- Supports: year/y/yr, quarter/qtr, month/mon/mons, week/w/wk,
--           day/d, hour/h/hr, minute/m/min, second/s/sec,
--           millisecond/ms, microsecond/us
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dateadd(part text, cnt integer, ts timestamp)
    RETURNS timestamp
    LANGUAGE plpgsql
    IMMUTABLE
AS $$
DECLARE
    lp text := lower(part);
BEGIN
    CASE lp
        WHEN 'year',   'y',   'yr',  'years'   THEN RETURN ts + (cnt || ' years')::interval;
        WHEN 'quarter','qtr'                     THEN RETURN ts + (cnt * 3 || ' months')::interval;
        WHEN 'month',  'mon', 'mons','months'   THEN RETURN ts + (cnt || ' months')::interval;
        WHEN 'week',   'w',   'wk',  'weeks'    THEN RETURN ts + (cnt * 7 || ' days')::interval;
        WHEN 'day',    'd',   'days'             THEN RETURN ts + (cnt || ' days')::interval;
        WHEN 'hour',   'h',   'hr',  'hours'    THEN RETURN ts + (cnt || ' hours')::interval;
        WHEN 'minute', 'm',   'min', 'minutes'  THEN RETURN ts + (cnt || ' minutes')::interval;
        WHEN 'second', 's',   'sec', 'seconds'  THEN RETURN ts + (cnt || ' seconds')::interval;
        WHEN 'millisecond','ms','milliseconds'  THEN RETURN ts + (cnt || ' milliseconds')::interval;
        WHEN 'microsecond','us','microseconds'  THEN RETURN ts + (cnt || ' microseconds')::interval;
        ELSE
            RAISE EXCEPTION 'dateadd: unknown datepart "%"', part;
    END CASE;
END;
$$;

-- Overload: accept date as third argument (auto-cast to timestamp)
CREATE OR REPLACE FUNCTION dateadd(part text, cnt integer, dt date)
    RETURNS timestamp
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT dateadd(part, cnt, dt::timestamp);
$$;

-- ---------------------------------------------------------------------------
-- 5.4 DATEDIFF(datepart text, start_ts timestamp, end_ts timestamp) → bigint
-- Boundary-crossing semantics (same as Redshift):
--   truncate both to the datepart unit, subtract.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION datediff(part text, start_ts timestamp, end_ts timestamp)
    RETURNS bigint
    LANGUAGE plpgsql
    IMMUTABLE
AS $$
DECLARE
    lp text := lower(part);
BEGIN
    CASE lp
        WHEN 'year','y','yr','years' THEN
            RETURN EXTRACT(YEAR FROM end_ts)::bigint - EXTRACT(YEAR FROM start_ts)::bigint;
        WHEN 'quarter','qtr' THEN
            RETURN (EXTRACT(YEAR FROM end_ts)::bigint * 4 + EXTRACT(QUARTER FROM end_ts)::bigint)
                 - (EXTRACT(YEAR FROM start_ts)::bigint * 4 + EXTRACT(QUARTER FROM start_ts)::bigint);
        WHEN 'month','mon','mons','months' THEN
            RETURN (EXTRACT(YEAR FROM end_ts)::bigint * 12 + EXTRACT(MONTH FROM end_ts)::bigint)
                 - (EXTRACT(YEAR FROM start_ts)::bigint * 12 + EXTRACT(MONTH FROM start_ts)::bigint);
        WHEN 'week','w','wk','weeks' THEN
            RETURN EXTRACT(EPOCH FROM DATE_TRUNC('week', end_ts) - DATE_TRUNC('week', start_ts))::bigint / 604800;
        WHEN 'day','d','days' THEN
            RETURN EXTRACT(EPOCH FROM DATE_TRUNC('day', end_ts) - DATE_TRUNC('day', start_ts))::bigint / 86400;
        WHEN 'hour','h','hr','hours' THEN
            RETURN EXTRACT(EPOCH FROM DATE_TRUNC('hour', end_ts) - DATE_TRUNC('hour', start_ts))::bigint / 3600;
        WHEN 'minute','m','min','minutes' THEN
            RETURN EXTRACT(EPOCH FROM DATE_TRUNC('minute', end_ts) - DATE_TRUNC('minute', start_ts))::bigint / 60;
        WHEN 'second','s','sec','seconds' THEN
            RETURN EXTRACT(EPOCH FROM DATE_TRUNC('second', end_ts) - DATE_TRUNC('second', start_ts))::bigint;
        WHEN 'millisecond','ms','milliseconds' THEN
            RETURN (EXTRACT(EPOCH FROM DATE_TRUNC('millisecond', end_ts) - DATE_TRUNC('millisecond', start_ts)) * 1000)::bigint;
        WHEN 'microsecond','us','microseconds' THEN
            RETURN (EXTRACT(EPOCH FROM end_ts - start_ts) * 1000000)::bigint;
        ELSE
            RAISE EXCEPTION 'datediff: unknown datepart "%"', part;
    END CASE;
END;
$$;

-- Overload: accept date arguments
CREATE OR REPLACE FUNCTION datediff(part text, start_dt date, end_dt date)
    RETURNS bigint
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT datediff(part, start_dt::timestamp, end_dt::timestamp);
$$;

-- ---------------------------------------------------------------------------
-- 5.5 CONVERT_TIMEZONE
-- 3-arg: (source_tz text, target_tz text, ts timestamp) → timestamp
-- 2-arg: (target_tz text, ts timestamp) → timestamp (assumes UTC source)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION convert_timezone(source_tz text, target_tz text, ts timestamp)
    RETURNS timestamp
    LANGUAGE sql
    STABLE
AS $$
    SELECT (ts AT TIME ZONE source_tz AT TIME ZONE target_tz)::timestamp;
$$;

CREATE OR REPLACE FUNCTION convert_timezone(target_tz text, ts timestamp)
    RETURNS timestamp
    LANGUAGE sql
    STABLE
AS $$
    SELECT convert_timezone('UTC', target_tz, ts);
$$;

-- ---------------------------------------------------------------------------
-- 5.6 ADD_MONTHS, MONTHS_BETWEEN, LAST_DAY, NEXT_DAY
-- ---------------------------------------------------------------------------

-- ADD_MONTHS(date, n integer) → date
CREATE OR REPLACE FUNCTION add_months(dt date, n integer)
    RETURNS date
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT (dt + (n || ' months')::interval)::date;
$$;

-- MONTHS_BETWEEN(date1, date2) → numeric (fractional months)
CREATE OR REPLACE FUNCTION months_between(d1 date, d2 date)
    RETURNS numeric
    LANGUAGE plpgsql
    IMMUTABLE
AS $$
DECLARE
    yr_diff  integer;
    mo_diff  integer;
    day_diff numeric;
BEGIN
    yr_diff  := EXTRACT(YEAR  FROM d1)::integer - EXTRACT(YEAR  FROM d2)::integer;
    mo_diff  := EXTRACT(MONTH FROM d1)::integer - EXTRACT(MONTH FROM d2)::integer;
    day_diff := (EXTRACT(DAY FROM d1) - EXTRACT(DAY FROM d2)) / 31.0;
    RETURN (yr_diff * 12 + mo_diff)::numeric + day_diff;
END;
$$;

-- LAST_DAY(date) → date  (last day of the month)
CREATE OR REPLACE FUNCTION last_day(dt date)
    RETURNS date
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT (DATE_TRUNC('month', dt) + INTERVAL '1 month' - INTERVAL '1 day')::date;
$$;

-- NEXT_DAY(date, day_name text) → date
-- Returns the next occurrence of day_name after dt.
CREATE OR REPLACE FUNCTION next_day(dt date, day_name text)
    RETURNS date
    LANGUAGE plpgsql
    IMMUTABLE
AS $$
DECLARE
    target_dow integer;
    current_dow integer;
    days_ahead  integer;
BEGIN
    target_dow := CASE lower(substring(day_name, 1, 3))
        WHEN 'sun' THEN 0
        WHEN 'mon' THEN 1
        WHEN 'tue' THEN 2
        WHEN 'wed' THEN 3
        WHEN 'thu' THEN 4
        WHEN 'fri' THEN 5
        WHEN 'sat' THEN 6
        ELSE NULL
    END;
    IF target_dow IS NULL THEN
        RAISE EXCEPTION 'next_day: unrecognized day name "%"', day_name;
    END IF;
    current_dow := EXTRACT(DOW FROM dt)::integer;
    days_ahead  := target_dow - current_dow;
    IF days_ahead <= 0 THEN
        days_ahead := days_ahead + 7;
    END IF;
    RETURN dt + days_ahead;
END;
$$;

-- ---------------------------------------------------------------------------
-- 5.7 Date/Timestamp comparison functions → -1, 0, or 1
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION date_cmp(d1 date, d2 date)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN d1 < d2 THEN -1 WHEN d1 > d2 THEN 1 ELSE 0 END;
$$;

CREATE OR REPLACE FUNCTION date_cmp_timestamp(d date, ts timestamp)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN d::timestamp < ts THEN -1 WHEN d::timestamp > ts THEN 1 ELSE 0 END;
$$;

CREATE OR REPLACE FUNCTION date_cmp_timestamptz(d date, tstz timestamptz)
    RETURNS integer LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN d::timestamptz < tstz THEN -1 WHEN d::timestamptz > tstz THEN 1 ELSE 0 END;
$$;

CREATE OR REPLACE FUNCTION timestamp_cmp(ts1 timestamp, ts2 timestamp)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN ts1 < ts2 THEN -1 WHEN ts1 > ts2 THEN 1 ELSE 0 END;
$$;

CREATE OR REPLACE FUNCTION timestamp_cmp_date(ts timestamp, d date)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN ts < d::timestamp THEN -1 WHEN ts > d::timestamp THEN 1 ELSE 0 END;
$$;

CREATE OR REPLACE FUNCTION timestamp_cmp_timestamptz(ts timestamp, tstz timestamptz)
    RETURNS integer LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN ts < tstz::timestamp THEN -1 WHEN ts > tstz::timestamp THEN 1 ELSE 0 END;
$$;

CREATE OR REPLACE FUNCTION timestamptz_cmp(tstz1 timestamptz, tstz2 timestamptz)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN tstz1 < tstz2 THEN -1 WHEN tstz1 > tstz2 THEN 1 ELSE 0 END;
$$;

CREATE OR REPLACE FUNCTION interval_cmp(i1 interval, i2 interval)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN i1 < i2 THEN -1 WHEN i1 > i2 THEN 1 ELSE 0 END;
$$;

-- ---------------------------------------------------------------------------
-- 5.8 DATE_PART_YEAR and TIMEOFDAY
-- ---------------------------------------------------------------------------

-- DATE_PART_YEAR(date) → integer
CREATE OR REPLACE FUNCTION date_part_year(dt date)
    RETURNS integer
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT EXTRACT(YEAR FROM dt)::integer;
$$;

-- TIMEOFDAY() → text  (current time as a formatted string)
CREATE OR REPLACE FUNCTION timeofday()
    RETURNS text
    LANGUAGE sql
    STABLE
AS $$
    SELECT TO_CHAR(CLOCK_TIMESTAMP(), 'Dy Mon DD HH24:MI:SS.US YYYY TZ');
$$;
