-- 01-implicit-casts.sql: Redshift-style implicit type casts from VARCHAR
-- Redshift allows VARCHAR columns/literals to be used directly in numeric,
-- date, and boolean expressions without explicit CAST. PostgreSQL requires
-- explicit casting by default; these CREATE CAST statements enable the same
-- permissive behavior.
--
-- NOTE: AS IMPLICIT means the planner will apply these casts automatically.
-- If ambiguity in function overload resolution arises, downgrade specific casts
-- to AS ASSIGNMENT without breaking explicit CAST usage.

-- VARCHAR → NUMERIC
CREATE CAST (varchar AS numeric)  WITH INOUT AS IMPLICIT;

-- VARCHAR → INTEGER
CREATE CAST (varchar AS integer)  WITH INOUT AS IMPLICIT;

-- VARCHAR → BIGINT
CREATE CAST (varchar AS bigint)   WITH INOUT AS IMPLICIT;

-- VARCHAR → DOUBLE PRECISION
CREATE CAST (varchar AS double precision) WITH INOUT AS IMPLICIT;

-- VARCHAR → DATE
CREATE CAST (varchar AS date)     WITH INOUT AS IMPLICIT;

-- VARCHAR → TIMESTAMP (without time zone)
CREATE CAST (varchar AS timestamp) WITH INOUT AS IMPLICIT;

-- VARCHAR → BOOLEAN
CREATE CAST (varchar AS boolean)  WITH INOUT AS IMPLICIT;
