-- 06-conditional-functions.sql: Redshift conditional function emulation

-- ---------------------------------------------------------------------------
-- 7.2 NVL(expr, replacement) → anyelement
-- Equivalent to COALESCE(expr, replacement).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION nvl(anyelement, anyelement)
    RETURNS anyelement
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT COALESCE($1, $2);
$$;

-- ---------------------------------------------------------------------------
-- 7.3 NVL2(expr, not_null_val, null_val) → anyelement
-- Returns not_null_val when expr is NOT NULL, otherwise null_val.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION nvl2(anyelement, anyelement, anyelement)
    RETURNS anyelement
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT CASE WHEN $1 IS NOT NULL THEN $2 ELSE $3 END;
$$;

-- ---------------------------------------------------------------------------
-- 7.4 DECODE overloads (TEXT, 3–11 arguments)
-- DECODE(expr, s1, r1 [, s2, r2, ...] [, default])
-- Returns rN for the first sN that equals expr; default (or NULL) otherwise.
--
-- Argument counts:
--   3  args: expr, s1, r1           → no default
--   4  args: expr, s1, r1, default
--   5  args: expr, s1, r1, s2, r2
--   6  args: expr, s1, r1, s2, r2, default
--   7  args: expr, s1, r1, s2, r2, s3, r3
--   ...up to 11 args (4 search/result pairs + default)
-- ---------------------------------------------------------------------------

-- 3 args: DECODE(expr, s1, r1)
CREATE OR REPLACE FUNCTION decode(text, text, text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN $1 IS NOT DISTINCT FROM $2 THEN $3 ELSE NULL END;
$$;

-- 4 args: DECODE(expr, s1, r1, default)
CREATE OR REPLACE FUNCTION decode(text, text, text, text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN $1 IS NOT DISTINCT FROM $2 THEN $3 ELSE $4 END;
$$;

-- 5 args: DECODE(expr, s1, r1, s2, r2)
CREATE OR REPLACE FUNCTION decode(text, text, text, text, text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN $1 IS NOT DISTINCT FROM $2 THEN $3
                WHEN $1 IS NOT DISTINCT FROM $4 THEN $5
                ELSE NULL END;
$$;

-- 6 args: DECODE(expr, s1, r1, s2, r2, default)
CREATE OR REPLACE FUNCTION decode(text, text, text, text, text, text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN $1 IS NOT DISTINCT FROM $2 THEN $3
                WHEN $1 IS NOT DISTINCT FROM $4 THEN $5
                ELSE $6 END;
$$;

-- 7 args: DECODE(expr, s1, r1, s2, r2, s3, r3)
CREATE OR REPLACE FUNCTION decode(text, text, text, text, text, text, text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN $1 IS NOT DISTINCT FROM $2 THEN $3
                WHEN $1 IS NOT DISTINCT FROM $4 THEN $5
                WHEN $1 IS NOT DISTINCT FROM $6 THEN $7
                ELSE NULL END;
$$;

-- 8 args: DECODE(expr, s1, r1, s2, r2, s3, r3, default)
CREATE OR REPLACE FUNCTION decode(text, text, text, text, text, text, text, text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN $1 IS NOT DISTINCT FROM $2 THEN $3
                WHEN $1 IS NOT DISTINCT FROM $4 THEN $5
                WHEN $1 IS NOT DISTINCT FROM $6 THEN $7
                ELSE $8 END;
$$;

-- 9 args: DECODE(expr, s1, r1, s2, r2, s3, r3, s4, r4)
CREATE OR REPLACE FUNCTION decode(text, text, text, text, text, text, text, text, text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN $1 IS NOT DISTINCT FROM $2 THEN $3
                WHEN $1 IS NOT DISTINCT FROM $4 THEN $5
                WHEN $1 IS NOT DISTINCT FROM $6 THEN $7
                WHEN $1 IS NOT DISTINCT FROM $8 THEN $9
                ELSE NULL END;
$$;

-- 10 args: DECODE(expr, s1, r1, s2, r2, s3, r3, s4, r4, default)
CREATE OR REPLACE FUNCTION decode(text, text, text, text, text, text, text, text, text, text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN $1 IS NOT DISTINCT FROM $2  THEN $3
                WHEN $1 IS NOT DISTINCT FROM $4  THEN $5
                WHEN $1 IS NOT DISTINCT FROM $6  THEN $7
                WHEN $1 IS NOT DISTINCT FROM $8  THEN $9
                ELSE $10 END;
$$;

-- 11 args: DECODE(expr, s1, r1, s2, r2, s3, r3, s4, r4, s5, r5)
CREATE OR REPLACE FUNCTION decode(text, text, text, text, text, text, text, text, text, text, text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN $1 IS NOT DISTINCT FROM $2  THEN $3
                WHEN $1 IS NOT DISTINCT FROM $4  THEN $5
                WHEN $1 IS NOT DISTINCT FROM $6  THEN $7
                WHEN $1 IS NOT DISTINCT FROM $8  THEN $9
                WHEN $1 IS NOT DISTINCT FROM $10 THEN $11
                ELSE NULL END;
$$;

-- 12 args: DECODE(expr, s1, r1, s2, r2, s3, r3, s4, r4, s5, r5, default)
CREATE OR REPLACE FUNCTION decode(text, text, text, text, text, text, text, text, text, text, text, text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN $1 IS NOT DISTINCT FROM $2  THEN $3
                WHEN $1 IS NOT DISTINCT FROM $4  THEN $5
                WHEN $1 IS NOT DISTINCT FROM $6  THEN $7
                WHEN $1 IS NOT DISTINCT FROM $8  THEN $9
                WHEN $1 IS NOT DISTINCT FROM $10 THEN $11
                ELSE $12 END;
$$;

-- ---------------------------------------------------------------------------
-- 7.5 ZEROIFNULL(numeric) and NULLIFZERO(numeric)
-- ---------------------------------------------------------------------------

-- ZEROIFNULL: returns 0 when expr is NULL, otherwise expr
CREATE OR REPLACE FUNCTION zeroifnull(v numeric)
    RETURNS numeric
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT COALESCE(v, 0);
$$;

-- NULLIFZERO: returns NULL when expr is 0, otherwise expr
CREATE OR REPLACE FUNCTION nullifzero(v numeric)
    RETURNS numeric
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT NULLIF(v, 0);
$$;
