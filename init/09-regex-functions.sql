-- 09-regex-functions.sql: Redshift regex function emulation

-- ---------------------------------------------------------------------------
-- 10.2 REGEXP_SUBSTR(str text, pattern text) → text
-- Returns the first substring matching pattern, or NULL if no match.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION regexp_substr(str text, pattern text)
    RETURNS text
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT (regexp_match(str, pattern))[1];
$$;

-- ---------------------------------------------------------------------------
-- 10.3 REGEXP_SUBSTR(str text, pattern text, startpos integer, occurrence integer) → text
-- Returns the Nth occurrence of pattern starting from 1-based startpos.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION regexp_substr(str text, pattern text, startpos integer, occurrence integer)
    RETURNS text
    LANGUAGE plpgsql
    IMMUTABLE
AS $$
DECLARE
    sub     text;
    i       integer := 0;
    m       record;
BEGIN
    sub := SUBSTR(str, startpos);
    FOR m IN SELECT * FROM regexp_matches(sub, pattern, 'g') LOOP
        i := i + 1;
        IF i = occurrence THEN
            RETURN m[1];
        END IF;
    END LOOP;
    RETURN NULL;
END;
$$;

-- ---------------------------------------------------------------------------
-- 10.4 REGEXP_COUNT(str text, pattern text) → integer
-- Returns the number of times pattern matches in str.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION regexp_count(str text, pattern text)
    RETURNS integer
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT count(*)::integer FROM regexp_matches(str, pattern, 'g');
$$;

-- ---------------------------------------------------------------------------
-- 10.5 REGEXP_INSTR(str text, pattern text) → integer
-- Returns 1-based position of first match, or 0 if not found.
-- PG 15+ has pg_catalog.regexp_instr natively; use it.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION regexp_instr(str text, pattern text)
    RETURNS integer
    LANGUAGE plpgsql
    IMMUTABLE
AS $$
DECLARE
    m text[];
BEGIN
    -- pg_catalog.regexp_instr available in PG 15+
    RETURN COALESCE(pg_catalog.regexp_instr(str, pattern), 0);
EXCEPTION WHEN undefined_function THEN
    -- Fallback: find first match via regexp_match, then strpos
    m := regexp_match(str, pattern);
    IF m IS NULL THEN RETURN 0; END IF;
    RETURN STRPOS(str, m[1]);
END;
$$;

-- ---------------------------------------------------------------------------
-- 10.6 REGEXP_REPLACE(str text, pattern text, replacement text, flags text) → text
-- 4-argument overload with PostgreSQL flag string (e.g., 'g' for global).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION regexp_replace(str text, pattern text, replacement text, flags text)
    RETURNS text
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT pg_catalog.regexp_replace(str, pattern, replacement, flags);
$$;
