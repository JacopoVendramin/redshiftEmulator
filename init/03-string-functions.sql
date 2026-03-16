-- 03-string-functions.sql: Redshift string function emulation

-- ---------------------------------------------------------------------------
-- 6.2 LEN(str text) → integer
-- Redshift LEN strips trailing spaces before computing length.
-- PostgreSQL LENGTH does not strip trailing spaces; CHAR_LENGTH doesn't either.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION len(str text)
    RETURNS integer
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT LENGTH(RTRIM(str));
$$;

-- ---------------------------------------------------------------------------
-- 6.3 CHARINDEX(substring text, string text) → integer
-- Returns 1-based position of first occurrence of substring in string.
-- Returns 0 if not found (Redshift/T-SQL convention; POSITION returns 0 too).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION charindex(substr text, str text)
    RETURNS integer
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT COALESCE(POSITION(substr IN str), 0);
$$;

-- ---------------------------------------------------------------------------
-- 6.4 REPLICATE(str text, n integer) → text
-- Equivalent to PostgreSQL REPEAT.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION replicate(str text, n integer)
    RETURNS text
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT REPEAT(str, n);
$$;

-- ---------------------------------------------------------------------------
-- 6.5 STRTOL(str text, base integer) → bigint
-- Converts a string representation of an integer in the given base to BIGINT.
-- Supports bases 2–36. Leading/trailing whitespace is tolerated.
-- Returns NULL for NULL input.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION strtol(str text, base integer)
    RETURNS bigint
    LANGUAGE plpgsql
    IMMUTABLE
AS $$
DECLARE
    s        text    := LOWER(TRIM(str));
    digits   text    := '0123456789abcdefghijklmnopqrstuvwxyz';
    result   bigint  := 0;
    negative boolean := false;
    c        char;
    digit_v  integer;
    i        integer;
BEGIN
    IF s IS NULL THEN RETURN NULL; END IF;
    IF base < 2 OR base > 36 THEN
        RAISE EXCEPTION 'strtol: base must be between 2 and 36, got %', base;
    END IF;

    -- Handle sign
    IF LEFT(s, 1) = '-' THEN
        negative := true;
        s := SUBSTR(s, 2);
    ELSIF LEFT(s, 1) = '+' THEN
        s := SUBSTR(s, 2);
    END IF;

    -- Optional 0x prefix for hex
    IF base = 16 AND LEFT(s, 2) = '0x' THEN
        s := SUBSTR(s, 3);
    END IF;

    FOR i IN 1..LENGTH(s) LOOP
        c := SUBSTR(s, i, 1);
        digit_v := POSITION(c IN digits) - 1;  -- 0-indexed
        IF digit_v < 0 OR digit_v >= base THEN
            RAISE EXCEPTION 'strtol: invalid character "%" for base %', c, base;
        END IF;
        result := result * base + digit_v;
    END LOOP;

    RETURN CASE WHEN negative THEN -result ELSE result END;
END;
$$;
