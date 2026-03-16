-- ============================================================
-- Smoke Test — runs at container startup via initdb
-- Tests all major UDF groups and emits a summary line.
-- ============================================================

DO $$
DECLARE
  _failures  INTEGER := 0;
  _total     INTEGER := 0;
  _got       TEXT;
  _expected  TEXT;
  _label     TEXT;

  -- ---- inline equality check --------------------------------
  -- Use: PERFORM _check('label', got::text, expected::text);
  -- We simulate it inline to avoid nested PROCEDURE issues.
BEGIN

  -- --------------------------------------------------------
  -- Helper macro (inline, repeated pattern):
  --   _total := _total + 1;
  --   IF _got IS DISTINCT FROM _expected THEN ...
  -- --------------------------------------------------------

  -- --------------------------------------------------------
  -- 1. Date/Time Functions
  -- --------------------------------------------------------

  _label := 'GETDATE returns timestamp';
  _got := pg_typeof(GETDATE())::text;
  _expected := 'timestamp without time zone';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'DATEADD year';
  _got := DATEADD('year', 1, '2020-01-01'::timestamp)::date::text;
  _expected := '2021-01-01';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'DATEADD mon abbreviation';
  _got := DATEADD('mon', 3, '2020-01-01'::timestamp)::date::text;
  _expected := '2020-04-01';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'DATEDIFF days';
  _got := DATEDIFF('day', '2020-01-01'::timestamp, '2020-01-11'::timestamp)::text;
  _expected := '10';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'DATEDIFF months';
  _got := DATEDIFF('month', '2020-01-01'::timestamp, '2020-04-01'::timestamp)::text;
  _expected := '3';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'ADD_MONTHS end-of-month';
  _got := ADD_MONTHS('2020-01-31'::date, 1)::text;
  _expected := '2020-02-29';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'MONTHS_BETWEEN';
  _got := round(MONTHS_BETWEEN('2020-04-01'::date, '2020-01-01'::date)::numeric, 0)::text;
  _expected := '3';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'LAST_DAY Feb leap';
  _got := LAST_DAY('2020-02-15'::date)::text;
  _expected := '2020-02-29';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'NEXT_DAY monday';
  _got := NEXT_DAY('2020-01-01'::date, 'monday')::text;
  _expected := '2020-01-06';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'DATE_PART_YEAR';
  _got := DATE_PART_YEAR('2023-07-15'::date)::text;
  _expected := '2023';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'DATE_CMP equal';
  _got := DATE_CMP('2020-01-01'::date, '2020-01-01'::date)::text;
  _expected := '0';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  -- --------------------------------------------------------
  -- 2. String Functions
  -- --------------------------------------------------------

  _label := 'LEN trailing spaces';
  _got := LEN('hello   ')::text;
  _expected := '5';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'CHARINDEX found';
  _got := CHARINDEX('ll', 'hello')::text;
  _expected := '3';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'CHARINDEX not found';
  _got := CHARINDEX('xyz', 'hello')::text;
  _expected := '0';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'REPLICATE';
  _got := REPLICATE('ab', 3);
  _expected := 'ababab';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'STRTOL hex';
  _got := STRTOL('ff', 16)::text;
  _expected := '255';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  -- --------------------------------------------------------
  -- 3. Conditional Functions
  -- --------------------------------------------------------

  _label := 'NVL not null';
  _got := NVL('a'::text, 'b');
  _expected := 'a';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'NVL null';
  _got := NVL(NULL::text, 'b');
  _expected := 'b';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'ZEROIFNULL null';
  _got := ZEROIFNULL(NULL)::text;
  _expected := '0';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'DECODE match';
  _got := DECODE('b'::text, 'a', 'first', 'b', 'second', 'default');
  _expected := 'second';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  -- --------------------------------------------------------
  -- 4. Aggregate Functions (use sub-select)
  -- --------------------------------------------------------

  _label := 'LISTAGG';
  SELECT LISTAGG(v::text, ',') INTO _got FROM (VALUES ('a'),('b'),('c')) t(v);
  _expected := 'a,b,c';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'MEDIAN odd';
  SELECT MEDIAN(v)::text INTO _got FROM (VALUES (1::numeric),(2),(3)) t(v);
  _expected := '2';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  -- --------------------------------------------------------
  -- 5. Array Functions
  -- --------------------------------------------------------

  _label := 'SPLIT_TO_ARRAY length';
  _got := array_length(SPLIT_TO_ARRAY('a,b,c', ','), 1)::text;
  _expected := '3';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'GET_ARRAY_LENGTH';
  _got := GET_ARRAY_LENGTH(ARRAY[1,2,3,4])::text;
  _expected := '4';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'SUBARRAY';
  _got := SUBARRAY(ARRAY[10,20,30,40,50], 2, 3)::text;
  _expected := '{20,30,40}';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  -- --------------------------------------------------------
  -- 6. Regex Functions
  -- --------------------------------------------------------

  _label := 'REGEXP_SUBSTR basic';
  _got := REGEXP_SUBSTR('abc123def', '[0-9]+');
  _expected := '123';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'REGEXP_COUNT';
  _got := REGEXP_COUNT('a1b2c3', '[0-9]')::text;
  _expected := '3';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'REGEXP_INSTR';
  _got := REGEXP_INSTR('abc123', '[0-9]+')::text;
  _expected := '4';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'REGEXP_REPLACE global';
  _got := REGEXP_REPLACE('hello world', 'o', '0', 'g');
  _expected := 'hell0 w0rld';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  -- --------------------------------------------------------
  -- 7. JSON Functions
  -- --------------------------------------------------------

  _label := 'JSON_EXTRACT_PATH_TEXT 2-key';
  _got := JSON_EXTRACT_PATH_TEXT('{"a":{"b":42}}', 'a', 'b');
  _expected := '42';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'JSON_EXTRACT_ARRAY_ELEMENT_TEXT';
  _got := JSON_EXTRACT_ARRAY_ELEMENT_TEXT('[10,20,30]', 1);
  _expected := '20';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'JSON_ARRAY_LENGTH';
  _got := JSON_ARRAY_LENGTH('[1,2,3]')::text;
  _expected := '3';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'CAN_JSON_PARSE valid';
  _got := CAN_JSON_PARSE('{"a":1}')::text;
  _expected := 'true';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'CAN_JSON_PARSE invalid';
  _got := CAN_JSON_PARSE('not json')::text;
  _expected := 'false';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'IS_VALID_JSON_ARRAY array';
  _got := IS_VALID_JSON_ARRAY('[1,2,3]')::text;
  _expected := 'true';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'IS_VALID_JSON_ARRAY object';
  _got := IS_VALID_JSON_ARRAY('{"a":1}')::text;
  _expected := 'false';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  -- --------------------------------------------------------
  -- 8. Math Functions
  -- --------------------------------------------------------

  _label := 'DEXP';
  _got := round(DEXP(1.0)::numeric, 5)::text;
  _expected := '2.71828';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'DLOG1';
  _got := round(DLOG1(2.718281828)::numeric, 4)::text;
  _expected := '1.0000';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'DLOG10';
  _got := round(DLOG10(100.0)::numeric, 4)::text;
  _expected := '2.0000';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  -- --------------------------------------------------------
  -- 9. Misc Functions
  -- --------------------------------------------------------

  _label := 'SLICE_NUM';
  _got := SLICE_NUM()::text;
  _expected := '0';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  _label := 'NODE_ID';
  _got := NODE_ID()::text;
  _expected := '0';
  _total := _total + 1;
  IF _got IS DISTINCT FROM _expected THEN
    _failures := _failures + 1;
    RAISE WARNING 'SMOKE FAIL [%]: got=% expected=%', _label, _got, _expected;
  END IF;

  -- --------------------------------------------------------
  -- Summary
  -- --------------------------------------------------------
  IF _failures = 0 THEN
    RAISE NOTICE 'SMOKE TEST: ALL PASSED (% tests)', _total;
  ELSE
    RAISE NOTICE 'SMOKE TEST: % FAILURES out of % tests', _failures, _total;
  END IF;

END;
$$;
