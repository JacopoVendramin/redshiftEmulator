-- 08-array-functions.sql: Redshift array function emulation

-- ---------------------------------------------------------------------------
-- 9.2 SPLIT_TO_ARRAY(str text, delimiter text) → text[]
-- Equivalent to string_to_array.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION split_to_array(str text, delimiter text)
    RETURNS text[]
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT string_to_array(str, delimiter);
$$;

-- ---------------------------------------------------------------------------
-- 9.3 GET_ARRAY_LENGTH(arr anyarray) → integer
-- Returns number of elements in the first dimension (NULL for empty array,
-- consistent with array_length behavior).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_array_length(arr anyarray)
    RETURNS integer
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT array_length(arr, 1);
$$;

-- ---------------------------------------------------------------------------
-- 9.4 SUBARRAY(arr anyarray, start integer, len integer) → anyarray
-- Returns a slice of arr starting at 1-based index start with len elements.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION subarray(arr anyarray, start_idx integer, len integer)
    RETURNS anyarray
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT arr[start_idx : start_idx + len - 1];
$$;

-- ---------------------------------------------------------------------------
-- 9.5 ARRAY_FLATTEN(arr anyarray) → anyarray
-- Flattens a nested array into a single-level array using unnest + array_agg.
-- Note: Works on 2D arrays; deeper nesting requires recursive unnesting.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION array_flatten(arr anyarray)
    RETURNS anyarray
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT array_agg(v) FROM unnest(arr) AS v;
$$;
