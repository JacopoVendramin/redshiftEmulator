-- ============================================================
-- Compatibility Test Suite
-- Deeper test cases (2-3 per function) including edge cases.
-- Run with: psql -h localhost -p 5439 -U admin -d dev -f tests/test-compatibility.sql
-- Expected: zero rows in the final failure summary.
-- ============================================================

-- Helper table for collecting failures
CREATE TEMP TABLE _test_results (
  test_name  TEXT,
  passed     BOOLEAN,
  got        TEXT,
  expected   TEXT
);

-- ============================================================
-- Macro helper: INSERT INTO _test_results(...)
-- ============================================================

-- ============================================================
-- 1. Date / Time Functions
-- ============================================================

INSERT INTO _test_results VALUES ('GETDATE() returns a timestamp',
  pg_typeof(GETDATE()) = 'timestamp without time zone'::regtype, pg_typeof(GETDATE())::text, 'timestamp without time zone');

INSERT INTO _test_results VALUES ('DATEADD year +2',
  DATEADD('year',2,'2020-03-15'::timestamp)::date = '2022-03-15'::date, DATEADD('year',2,'2020-03-15'::timestamp)::date::text, '2022-03-15');

INSERT INTO _test_results VALUES ('DATEADD quarter',
  DATEADD('qtr',2,'2020-01-01'::timestamp)::date = '2020-07-01'::date, DATEADD('qtr',2,'2020-01-01'::timestamp)::date::text, '2020-07-01');

INSERT INTO _test_results VALUES ('DATEADD negative day',
  DATEADD('day',-5,'2020-01-10'::timestamp)::date = '2020-01-05'::date, DATEADD('day',-5,'2020-01-10'::timestamp)::date::text, '2020-01-05');

INSERT INTO _test_results VALUES ('DATEDIFF hours',
  DATEDIFF('hour','2020-01-01 00:00:00'::timestamp,'2020-01-01 06:30:00'::timestamp) = 6, DATEDIFF('hour','2020-01-01 00:00:00'::timestamp,'2020-01-01 06:30:00'::timestamp)::text, '6');

INSERT INTO _test_results VALUES ('DATEDIFF weeks',
  DATEDIFF('week','2020-01-01'::timestamp,'2020-01-22'::timestamp) = 3, DATEDIFF('week','2020-01-01'::timestamp,'2020-01-22'::timestamp)::text, '3');

INSERT INTO _test_results VALUES ('DATEDIFF years',
  DATEDIFF('year','2018-12-31'::timestamp,'2021-01-01'::timestamp) = 3, DATEDIFF('year','2018-12-31'::timestamp,'2021-01-01'::timestamp)::text, '3');

INSERT INTO _test_results VALUES ('CONVERT_TIMEZONE UTC to NY summer',
  DATE_PART('hour', CONVERT_TIMEZONE('UTC','America/New_York','2020-07-01 12:00:00'::timestamp)) = 8,
  DATE_PART('hour', CONVERT_TIMEZONE('UTC','America/New_York','2020-07-01 12:00:00'::timestamp))::text, '8');

INSERT INTO _test_results VALUES ('CONVERT_TIMEZONE 2-arg (source=UTC)',
  DATE_PART('hour', CONVERT_TIMEZONE('Europe/London','2020-01-01 12:00:00'::timestamp)) = 12,
  DATE_PART('hour', CONVERT_TIMEZONE('Europe/London','2020-01-01 12:00:00'::timestamp))::text, '12');

INSERT INTO _test_results VALUES ('ADD_MONTHS end-of-month Jan→Feb leap',
  ADD_MONTHS('2020-01-31'::date,1) = '2020-02-29'::date, ADD_MONTHS('2020-01-31'::date,1)::text, '2020-02-29');

INSERT INTO _test_results VALUES ('ADD_MONTHS +12 same day',
  ADD_MONTHS('2020-03-15'::date,12) = '2021-03-15'::date, ADD_MONTHS('2020-03-15'::date,12)::text, '2021-03-15');

INSERT INTO _test_results VALUES ('MONTHS_BETWEEN positive',
  MONTHS_BETWEEN('2020-07-01'::date,'2020-01-01'::date) = 6, MONTHS_BETWEEN('2020-07-01'::date,'2020-01-01'::date)::text, '6');

INSERT INTO _test_results VALUES ('MONTHS_BETWEEN negative',
  MONTHS_BETWEEN('2020-01-01'::date,'2020-07-01'::date) = -6, MONTHS_BETWEEN('2020-01-01'::date,'2020-07-01'::date)::text, '-6');

INSERT INTO _test_results VALUES ('LAST_DAY Feb non-leap',
  LAST_DAY('2019-02-10'::date) = '2019-02-28'::date, LAST_DAY('2019-02-10'::date)::text, '2019-02-28');

INSERT INTO _test_results VALUES ('LAST_DAY December',
  LAST_DAY('2020-12-01'::date) = '2020-12-31'::date, LAST_DAY('2020-12-01'::date)::text, '2020-12-31');

INSERT INTO _test_results VALUES ('NEXT_DAY wednesday',
  NEXT_DAY('2020-01-01'::date,'wednesday') = '2020-01-08'::date, NEXT_DAY('2020-01-01'::date,'wednesday')::text, '2020-01-08');

INSERT INTO _test_results VALUES ('DATE_PART_YEAR',
  DATE_PART_YEAR('2023-11-05'::date) = 2023, DATE_PART_YEAR('2023-11-05'::date)::text, '2023');

INSERT INTO _test_results VALUES ('DATE_CMP greater',
  DATE_CMP('2021-01-01'::date,'2020-01-01'::date) = 1, DATE_CMP('2021-01-01'::date,'2020-01-01'::date)::text, '1');

INSERT INTO _test_results VALUES ('TIMESTAMP_CMP equal',
  TIMESTAMP_CMP('2020-06-15 10:00:00'::timestamp,'2020-06-15 10:00:00'::timestamp) = 0,
  TIMESTAMP_CMP('2020-06-15 10:00:00'::timestamp,'2020-06-15 10:00:00'::timestamp)::text, '0');

-- ============================================================
-- 2. String Functions
-- ============================================================

INSERT INTO _test_results VALUES ('LEN empty string',
  LEN('') = 0, LEN('')::text, '0');

INSERT INTO _test_results VALUES ('LEN all spaces',
  LEN('   ') = 0, LEN('   ')::text, '0');

INSERT INTO _test_results VALUES ('LEN normal',
  LEN('hello') = 5, LEN('hello')::text, '5');

INSERT INTO _test_results VALUES ('CHARINDEX at start',
  CHARINDEX('he','hello') = 1, CHARINDEX('he','hello')::text, '1');

INSERT INTO _test_results VALUES ('CHARINDEX case sensitive',
  CHARINDEX('HE','hello') = 0, CHARINDEX('HE','hello')::text, '0');

INSERT INTO _test_results VALUES ('CHARINDEX empty needle',
  CHARINDEX('','hello') = 1, CHARINDEX('','hello')::text, '1');

INSERT INTO _test_results VALUES ('REPLICATE 0 times',
  REPLICATE('abc',0) = '', REPLICATE('abc',0), '');

INSERT INTO _test_results VALUES ('REPLICATE 1 time',
  REPLICATE('ab',1) = 'ab', REPLICATE('ab',1), 'ab');

INSERT INTO _test_results VALUES ('STRTOL octal',
  STRTOL('17',8) = 15, STRTOL('17',8)::text, '15');

INSERT INTO _test_results VALUES ('STRTOL base 10',
  STRTOL('255',10) = 255, STRTOL('255',10)::text, '255');

-- ============================================================
-- 3. Conditional Functions
-- ============================================================

INSERT INTO _test_results VALUES ('NVL both non-null returns first',
  NVL('a'::text,'b') = 'a', NVL('a'::text,'b'), 'a');

INSERT INTO _test_results VALUES ('NVL2 null expr1 → expr3',
  NVL2(NULL::text,'yes'::text,'no') = 'no', NVL2(NULL::text,'yes'::text,'no'), 'no');

INSERT INTO _test_results VALUES ('ZEROIFNULL zero passthrough',
  ZEROIFNULL(0) = 0, ZEROIFNULL(0)::text, '0');

INSERT INTO _test_results VALUES ('NULLIFZERO non-zero passthrough',
  NULLIFZERO(5) = 5, NULLIFZERO(5)::text, '5');

INSERT INTO _test_results VALUES ('DECODE no match no default → NULL',
  DECODE('z'::text,'a','first','b','second') IS NULL, DECODE('z'::text,'a','first','b','second'), NULL);

INSERT INTO _test_results VALUES ('DECODE NULL search matches NULL',
  DECODE(NULL::text, NULL::text, 'match', 'no') = 'match', DECODE(NULL::text, NULL::text, 'match', 'no'), 'match');

-- ============================================================
-- 4. Aggregate Functions
-- ============================================================

INSERT INTO _test_results
SELECT 'LISTAGG with single value', v = 'only', v, 'only'
FROM (SELECT LISTAGG(x::text,',') v FROM (VALUES ('only')) t(x)) s;

INSERT INTO _test_results
SELECT 'LISTAGG empty delimiter', v = 'ab', v, 'ab'
FROM (SELECT LISTAGG(x::text,'') v FROM (VALUES ('a'),('b')) t(x)) s;

INSERT INTO _test_results
SELECT 'MEDIAN single value', v = 7, v::text, '7'
FROM (SELECT MEDIAN(x) v FROM (VALUES (7::numeric)) t(x)) s;

INSERT INTO _test_results
SELECT 'MEDIAN with NULLs ignored', v = 3, v::text, '3'
FROM (SELECT MEDIAN(x) v FROM (VALUES (1::numeric),(3),(5),(NULL)) t(x)) s;

-- ============================================================
-- 5. Array Functions
-- ============================================================

INSERT INTO _test_results VALUES ('SPLIT_TO_ARRAY single element',
  SPLIT_TO_ARRAY('hello',',') = ARRAY['hello'], SPLIT_TO_ARRAY('hello',',')::text, '{hello}');

INSERT INTO _test_results VALUES ('GET_ARRAY_LENGTH empty',
  GET_ARRAY_LENGTH(ARRAY[]::int[]) IS NULL, GET_ARRAY_LENGTH(ARRAY[]::int[])::text, NULL);

INSERT INTO _test_results VALUES ('SUBARRAY start beyond end → empty',
  array_length(SUBARRAY(ARRAY[1,2,3],5,0),1) IS NULL, array_length(SUBARRAY(ARRAY[1,2,3],5,0),1)::text, NULL);

INSERT INTO _test_results VALUES ('SUBARRAY from index 1',
  SUBARRAY(ARRAY[10,20,30],1,2) = ARRAY[10,20], SUBARRAY(ARRAY[10,20,30],1,2)::text, '{10,20}');

-- ============================================================
-- 6. Regex Functions
-- ============================================================

INSERT INTO _test_results VALUES ('REGEXP_SUBSTR nth occurrence',
  REGEXP_SUBSTR('a1b2c3','[0-9]+',1,2) = '2', REGEXP_SUBSTR('a1b2c3','[0-9]+',1,2), '2');

INSERT INTO _test_results VALUES ('REGEXP_SUBSTR out-of-bounds occurrence',
  REGEXP_SUBSTR('a1b2','[0-9]+',1,5) IS NULL, REGEXP_SUBSTR('a1b2','[0-9]+',1,5), NULL);

INSERT INTO _test_results VALUES ('REGEXP_COUNT no match',
  REGEXP_COUNT('abc','[0-9]') = 0, REGEXP_COUNT('abc','[0-9]')::text, '0');

INSERT INTO _test_results VALUES ('REGEXP_INSTR no match → 0',
  REGEXP_INSTR('abc','[0-9]+') = 0, REGEXP_INSTR('abc','[0-9]+')::text, '0');

INSERT INTO _test_results VALUES ('REGEXP_REPLACE first only (no g flag)',
  REGEXP_REPLACE('aaa','a','X','') = 'Xaa', REGEXP_REPLACE('aaa','a','X',''), 'Xaa');

INSERT INTO _test_results VALUES ('REGEXP_REPLACE global',
  REGEXP_REPLACE('aaa','a','X','g') = 'XXX', REGEXP_REPLACE('aaa','a','X','g'), 'XXX');

-- ============================================================
-- 7. JSON Functions
-- ============================================================

INSERT INTO _test_results VALUES ('JSON_EXTRACT_PATH_TEXT missing key → NULL',
  JSON_EXTRACT_PATH_TEXT('{"a":1}','z') IS NULL, JSON_EXTRACT_PATH_TEXT('{"a":1}','z'), NULL);

INSERT INTO _test_results VALUES ('JSON_EXTRACT_PATH_TEXT 3-key',
  JSON_EXTRACT_PATH_TEXT('{"a":{"b":{"c":"deep"}}}','a','b','c') = 'deep',
  JSON_EXTRACT_PATH_TEXT('{"a":{"b":{"c":"deep"}}}','a','b','c'), 'deep');

INSERT INTO _test_results VALUES ('JSON_EXTRACT_ARRAY_ELEMENT_TEXT last',
  JSON_EXTRACT_ARRAY_ELEMENT_TEXT('[10,20,30]',2) = '30', JSON_EXTRACT_ARRAY_ELEMENT_TEXT('[10,20,30]',2), '30');

INSERT INTO _test_results VALUES ('JSON_EXTRACT_ARRAY_ELEMENT_TEXT out-of-bounds → NULL',
  JSON_EXTRACT_ARRAY_ELEMENT_TEXT('[1,2]',5) IS NULL, JSON_EXTRACT_ARRAY_ELEMENT_TEXT('[1,2]',5), NULL);

INSERT INTO _test_results VALUES ('JSON_ARRAY_LENGTH nested',
  JSON_ARRAY_LENGTH('[[1,2],[3,4],[5]]') = 3, JSON_ARRAY_LENGTH('[[1,2],[3,4],[5]]')::text, '3');

INSERT INTO _test_results VALUES ('CAN_JSON_PARSE empty string false',
  CAN_JSON_PARSE('') = FALSE, CAN_JSON_PARSE('')::text, 'false');

INSERT INTO _test_results VALUES ('IS_VALID_JSON_ARRAY empty array',
  IS_VALID_JSON_ARRAY('[]') = TRUE, IS_VALID_JSON_ARRAY('[]')::text, 'true');

INSERT INTO _test_results VALUES ('IS_VALID_JSON_ARRAY invalid JSON',
  IS_VALID_JSON_ARRAY('bad') = FALSE, IS_VALID_JSON_ARRAY('bad')::text, 'false');

-- ============================================================
-- 8. Math Functions
-- ============================================================

INSERT INTO _test_results VALUES ('DEXP(0) = 1',
  DEXP(0.0) = 1.0, DEXP(0.0)::text, '1');

INSERT INTO _test_results VALUES ('DLOG1(1) = 0',
  DLOG1(1.0) = 0.0, DLOG1(1.0)::text, '0');

INSERT INTO _test_results VALUES ('DLOG10(1000) = 3',
  round(DLOG10(1000.0)::numeric, 5) = 3.00000, round(DLOG10(1000.0)::numeric,5)::text, '3.00000');

-- ============================================================
-- 9. Implicit Casts (task 18.2)
-- Test in WHERE clause, arithmetic, and JOIN-like patterns.
-- ============================================================

-- WHERE clause: VARCHAR literal compared to NUMERIC column
CREATE TEMP TABLE _cast_test (id INTEGER, amount NUMERIC);
INSERT INTO _cast_test VALUES (1, 100), (2, 200.50), (3, 0);

INSERT INTO _test_results
SELECT 'Implicit cast: VARCHAR to NUMERIC in WHERE',
  COUNT(*) = 1, COUNT(*)::text, '1'
FROM _cast_test WHERE amount = '200.50';

INSERT INTO _test_results
SELECT 'Implicit cast: VARCHAR to INTEGER in arithmetic',
  (1 + '4'::integer) = 5, (1 + '4'::integer)::text, '5';

INSERT INTO _test_results
SELECT 'Implicit cast: VARCHAR to DATE in WHERE',
  COUNT(*) = 1, COUNT(*)::text, '1'
FROM (VALUES ('2020-01-01'::date)) t(d) WHERE d = '2020-01-01';

-- ============================================================
-- Report
-- ============================================================

SELECT
  CASE WHEN COUNT(*) = 0 THEN 'COMPATIBILITY TESTS: ALL PASSED'
       ELSE 'COMPATIBILITY TESTS: ' || COUNT(*) || ' FAILURES'
  END AS summary
FROM _test_results
WHERE NOT passed;

-- Show detail for any failures
SELECT test_name, got, expected
FROM _test_results
WHERE NOT passed
ORDER BY test_name;

DROP TABLE _cast_test;
DROP TABLE _test_results;
