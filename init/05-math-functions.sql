-- ============================================================
-- Redshift Math Functions Emulation
-- ============================================================

-- ------------------------------------------------------------
-- DEXP(n) → EXP(n)
-- Returns e raised to the power of n (double precision).
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION DEXP(n DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
LANGUAGE SQL
IMMUTABLE STRICT
AS $$
  SELECT EXP(n);
$$;

-- ------------------------------------------------------------
-- DLOG1(n) → LN(n)
-- Returns the natural logarithm of n.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION DLOG1(n DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
LANGUAGE SQL
IMMUTABLE STRICT
AS $$
  SELECT LN(n);
$$;

-- ------------------------------------------------------------
-- DLOG10(n) → LOG10(n)
-- Returns the base-10 logarithm of n.
-- PostgreSQL's LOG(b, x) computes log base b of x.
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION DLOG10(n DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
LANGUAGE SQL
IMMUTABLE STRICT
AS $$
  SELECT LOG(10, n::numeric)::double precision;
$$;
