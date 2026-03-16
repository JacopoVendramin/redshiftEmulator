-- 07-aggregate-functions.sql: Redshift aggregate function emulation

-- ---------------------------------------------------------------------------
-- 8.2 LISTAGG(text, delimiter) aggregate
-- Backed by string_agg internal functions.
-- WITHIN GROUP (ORDER BY ...) syntax is NOT supported as a custom aggregate
-- (requires ordered-set aggregate C support). Known limitation.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE AGGREGATE listagg(text, text) (
    SFUNC     = string_agg_transfn,
    STYPE     = internal,
    FINALFUNC = string_agg_finalfn,
    PARALLEL  = SAFE
);

-- ---------------------------------------------------------------------------
-- 8.3 MEDIAN(numeric) → numeric
-- Implemented as a custom aggregate that accumulates values in an array,
-- then computes the median via PERCENTILE_CONT in the final function.
-- ---------------------------------------------------------------------------

-- State transition: accumulate values into a numeric array
CREATE OR REPLACE FUNCTION _median_transfn(state numeric[], val numeric)
    RETURNS numeric[]
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT CASE WHEN val IS NULL THEN state ELSE array_append(state, val) END;
$$;

-- Final function: compute median from accumulated array
CREATE OR REPLACE FUNCTION _median_finalfn(state numeric[])
    RETURNS numeric
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v)
    FROM unnest(state) AS v;
$$;

-- MEDIAN aggregate
CREATE OR REPLACE AGGREGATE median(numeric) (
    SFUNC     = _median_transfn,
    STYPE     = numeric[],
    FINALFUNC = _median_finalfn,
    INITCOND  = '{}'
);
