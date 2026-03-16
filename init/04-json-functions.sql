-- ============================================================
-- Redshift JSON Functions Emulation
-- ============================================================

-- ------------------------------------------------------------
-- JSON_EXTRACT_PATH_TEXT: navigate JSON path (string keys)
-- Redshift signature: JSON_EXTRACT_PATH_TEXT(json, path_elem, ...)
-- Returns TEXT value at the specified path, or NULL.
-- Overloads for 1 through 5 key arguments (most common usage).
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION JSON_EXTRACT_PATH_TEXT(json_string TEXT, k1 TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT (json_string::jsonb -> k1)::text::text;
$$;

-- Strip enclosing quotes from a jsonb scalar value
CREATE OR REPLACE FUNCTION _jsonb_extract_text(v jsonb)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT CASE
    WHEN v IS NULL THEN NULL
    WHEN jsonb_typeof(v) = 'string' THEN v #>> '{}'
    ELSE v::text
  END;
$$;

-- Replace the 1-key variant to return clean scalar text (no quotes)
CREATE OR REPLACE FUNCTION JSON_EXTRACT_PATH_TEXT(json_string TEXT, k1 TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT _jsonb_extract_text(json_string::jsonb -> k1);
$$;

CREATE OR REPLACE FUNCTION JSON_EXTRACT_PATH_TEXT(json_string TEXT, k1 TEXT, k2 TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT _jsonb_extract_text(json_string::jsonb -> k1 -> k2);
$$;

CREATE OR REPLACE FUNCTION JSON_EXTRACT_PATH_TEXT(json_string TEXT, k1 TEXT, k2 TEXT, k3 TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT _jsonb_extract_text(json_string::jsonb -> k1 -> k2 -> k3);
$$;

CREATE OR REPLACE FUNCTION JSON_EXTRACT_PATH_TEXT(json_string TEXT, k1 TEXT, k2 TEXT, k3 TEXT, k4 TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT _jsonb_extract_text(json_string::jsonb -> k1 -> k2 -> k3 -> k4);
$$;

CREATE OR REPLACE FUNCTION JSON_EXTRACT_PATH_TEXT(json_string TEXT, k1 TEXT, k2 TEXT, k3 TEXT, k4 TEXT, k5 TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT _jsonb_extract_text(json_string::jsonb -> k1 -> k2 -> k3 -> k4 -> k5);
$$;

-- ------------------------------------------------------------
-- JSON_EXTRACT_ARRAY_ELEMENT_TEXT(json_string, pos)
-- Returns TEXT of the element at 0-based index pos in a JSON array.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION JSON_EXTRACT_ARRAY_ELEMENT_TEXT(json_string TEXT, pos INTEGER)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT _jsonb_extract_text(json_string::jsonb -> pos);
$$;

-- ------------------------------------------------------------
-- JSON_ARRAY_LENGTH(json_string)
-- Returns the number of elements in the top-level JSON array.
-- Returns NULL if the input is not a JSON array.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION JSON_ARRAY_LENGTH(json_string TEXT)
RETURNS INTEGER
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT CASE
    WHEN json_string IS NULL THEN NULL
    WHEN jsonb_typeof(json_string::jsonb) = 'array' THEN jsonb_array_length(json_string::jsonb)
    ELSE NULL
  END;
$$;

-- ------------------------------------------------------------
-- JSON_PARSE(json_string) → JSONB
-- Parses a JSON string and returns JSONB.
-- Raises an error on invalid JSON (same as Redshift).
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION JSON_PARSE(json_string TEXT)
RETURNS JSONB
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT json_string::jsonb;
$$;

-- ------------------------------------------------------------
-- JSON_SERIALIZE(jsonb) → TEXT
-- Serializes a JSONB value back to TEXT.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION JSON_SERIALIZE(val JSONB)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT val::text;
$$;

-- ------------------------------------------------------------
-- CAN_JSON_PARSE(json_string) → BOOLEAN
-- Returns TRUE if the string is valid JSON, FALSE otherwise.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION CAN_JSON_PARSE(json_string TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  PERFORM json_string::jsonb;
  RETURN TRUE;
EXCEPTION WHEN others THEN
  RETURN FALSE;
END;
$$;

-- ------------------------------------------------------------
-- IS_VALID_JSON(json_string) → BOOLEAN
-- Alias for CAN_JSON_PARSE.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION IS_VALID_JSON(json_string TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  PERFORM json_string::jsonb;
  RETURN TRUE;
EXCEPTION WHEN others THEN
  RETURN FALSE;
END;
$$;

-- ------------------------------------------------------------
-- IS_VALID_JSON_ARRAY(json_string) → BOOLEAN
-- Returns TRUE if the string is valid JSON AND is an array.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION IS_VALID_JSON_ARRAY(json_string TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v jsonb;
BEGIN
  v := json_string::jsonb;
  RETURN jsonb_typeof(v) = 'array';
EXCEPTION WHEN others THEN
  RETURN FALSE;
END;
$$;
